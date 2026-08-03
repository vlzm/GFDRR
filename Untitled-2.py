import argparse
import copy
import random
import time
from pathlib import Path

import numpy as np
import pandas as pd
import torch
import torch.nn as nn
import torch.nn.functional as F
from sklearn.metrics import (
    accuracy_score,
    classification_report,
    confusion_matrix,
    f1_score,
)
from torch.utils.data import DataLoader, random_split
from torchvision import datasets, transforms


# ----------------------------------------------------------------------------- #
# 0. Утилиты
# ----------------------------------------------------------------------------- #
def set_seed(seed: int = 42) -> None:
    """Фиксируем сиды для воспроизводимости."""
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    torch.cuda.manual_seed_all(seed)


def get_device() -> torch.device:
    if torch.cuda.is_available():
        return torch.device("cuda")
    if torch.backends.mps.is_available():  # Apple Silicon
        return torch.device("mps")
    return torch.device("cpu")


# ----------------------------------------------------------------------------- #
# 1. Данные: загрузка + предобработка
# ----------------------------------------------------------------------------- #
# Статистики CIFAR-10 (посчитаны по train-части датасета)
CIFAR_MEAN = (0.4914, 0.4822, 0.4465)
CIFAR_STD = (0.2470, 0.2435, 0.2616)

CLASS_NAMES = [
    "airplane", "automobile", "bird", "cat", "deer",
    "dog", "frog", "horse", "ship", "truck",
]


def build_datasets(data_dir: str, val_fraction: float = 0.1, seed: int = 42):
    """Скачиваем CIFAR-10 и делим train на train/val.

    Train — с аугментациями, val/test — только нормализация.
    """
    train_tf = transforms.Compose([
        transforms.RandomCrop(32, padding=4),
        transforms.RandomHorizontalFlip(),
        transforms.ToTensor(),
        transforms.Normalize(CIFAR_MEAN, CIFAR_STD),
    ])
    eval_tf = transforms.Compose([
        transforms.ToTensor(),
        transforms.Normalize(CIFAR_MEAN, CIFAR_STD),
    ])

    full_train = datasets.CIFAR10(data_dir, train=True, download=True, transform=train_tf)
    full_train_eval = datasets.CIFAR10(data_dir, train=True, download=False, transform=eval_tf)
    test_ds = datasets.CIFAR10(data_dir, train=False, download=True, transform=eval_tf)

    n_total = len(full_train)
    n_val = int(n_total * val_fraction)
    n_train = n_total - n_val

    gen = torch.Generator().manual_seed(seed)
    train_subset, _ = random_split(full_train, [n_train, n_val], generator=gen)
    gen = torch.Generator().manual_seed(seed)
    _, val_subset = random_split(full_train_eval, [n_train, n_val], generator=gen)

    return train_subset, val_subset, test_ds


def build_loaders(train_ds, val_ds, test_ds, batch_size: int, num_workers: int = 2):
    """DataLoader'ы. pin_memory ускоряет копирование на GPU."""
    common = dict(num_workers=num_workers, pin_memory=torch.cuda.is_available())
    train_loader = DataLoader(train_ds, batch_size=batch_size, shuffle=True, drop_last=True, **common)
    val_loader = DataLoader(val_ds, batch_size=batch_size * 2, shuffle=False, **common)
    test_loader = DataLoader(test_ds, batch_size=batch_size * 2, shuffle=False, **common)
    return train_loader, val_loader, test_loader


# ----------------------------------------------------------------------------- #
# 2. Модель
# ----------------------------------------------------------------------------- #
class ConvBlock(nn.Module):
    """Conv -> BN -> ReLU (x2) -> MaxPool."""

    def __init__(self, in_ch: int, out_ch: int):
        super().__init__()
        self.block = nn.Sequential(
            nn.Conv2d(in_ch, out_ch, kernel_size=3, padding=1, bias=False),
            nn.BatchNorm2d(out_ch),
            nn.ReLU(inplace=True),
            nn.Conv2d(out_ch, out_ch, kernel_size=3, padding=1, bias=False),
            nn.BatchNorm2d(out_ch),
            nn.ReLU(inplace=True),
            nn.MaxPool2d(2),
        )

    def forward(self, x):
        return self.block(x)


class SimpleCNN(nn.Module):
    """Небольшая VGG-подобная сеть: ~2.5M параметров, на CIFAR-10 даёт ~90%."""

    def __init__(self, num_classes: int = 10, dropout: float = 0.3):
        super().__init__()
        self.features = nn.Sequential(
            ConvBlock(3, 64),    # 32x32 -> 16x16
            ConvBlock(64, 128),  # 16x16 -> 8x8
            ConvBlock(128, 256), # 8x8   -> 4x4
        )
        self.classifier = nn.Sequential(
            nn.AdaptiveAvgPool2d(1),
            nn.Flatten(),
            nn.Dropout(dropout),
            nn.Linear(256, 128),
            nn.ReLU(inplace=True),
            nn.Dropout(dropout),
            nn.Linear(128, num_classes),
        )

    def forward(self, x):
        x = self.features(x)
        return self.classifier(x)


# ----------------------------------------------------------------------------- #
# 3. Обучение и валидация
# ----------------------------------------------------------------------------- #
def run_epoch(model, loader, criterion, device, optimizer=None):
    """Один проход по данным. Если optimizer=None — режим валидации."""
    is_train = optimizer is not None
    model.train(is_train)

    total_loss, total_correct, total_samples = 0.0, 0, 0

    with torch.set_grad_enabled(is_train):
        for images, targets in loader:
            images = images.to(device, non_blocking=True)
            targets = targets.to(device, non_blocking=True)

            logits = model(images)
            loss = criterion(logits, targets)

            if is_train:
                optimizer.zero_grad(set_to_none=True)
                loss.backward()
                optimizer.step()

            batch = targets.size(0)
            total_loss += loss.item() * batch
            total_correct += (logits.argmax(dim=1) == targets).sum().item()
            total_samples += batch

    return total_loss / total_samples, total_correct / total_samples


def train(model, train_loader, val_loader, device, epochs, lr, weight_decay, patience, ckpt_path):
    """Полный цикл обучения: AdamW + cosine schedule + early stopping."""
    criterion = nn.CrossEntropyLoss(label_smoothing=0.1)
    optimizer = torch.optim.AdamW(model.parameters(), lr=lr, weight_decay=weight_decay)
    scheduler = torch.optim.lr_scheduler.CosineAnnealingLR(optimizer, T_max=epochs)

    history = []
    best_val_acc = 0.0
    best_state = copy.deepcopy(model.state_dict())
    epochs_no_improve = 0

    for epoch in range(1, epochs + 1):
        t0 = time.time()
        train_loss, train_acc = run_epoch(model, train_loader, criterion, device, optimizer)
        val_loss, val_acc = run_epoch(model, val_loader, criterion, device)
        scheduler.step()

        history.append({
            "epoch": epoch,
            "train_loss": train_loss,
            "train_acc": train_acc,
            "val_loss": val_loss,
            "val_acc": val_acc,
            "lr": scheduler.get_last_lr()[0],
            "time_sec": time.time() - t0,
        })

        marker = ""
        if val_acc > best_val_acc:
            best_val_acc = val_acc
            best_state = copy.deepcopy(model.state_dict())
            torch.save({"model_state": best_state, "val_acc": best_val_acc}, ckpt_path)
            epochs_no_improve = 0
            marker = "  <- best"
        else:
            epochs_no_improve += 1

        print(
            f"[{epoch:03d}/{epochs}] "
            f"train_loss={train_loss:.4f} train_acc={train_acc:.4f} | "
            f"val_loss={val_loss:.4f} val_acc={val_acc:.4f} | "
            f"{history[-1]['time_sec']:.1f}s{marker}"
        )

        if epochs_no_improve >= patience:
            print(f"Early stopping: val_acc не улучшается {patience} эпох подряд.")
            break

    model.load_state_dict(best_state)  # возвращаем лучшие веса
    return model, pd.DataFrame(history), best_val_acc


# ----------------------------------------------------------------------------- #
# 4. Постобработка: инференс + метрики
# ----------------------------------------------------------------------------- #
@torch.no_grad()
def predict(model, loader, device):
    """Собираем предсказания и вероятности по всему лоадеру."""
    model.eval()
    all_preds, all_targets, all_probs = [], [], []
    for images, targets in loader:
        images = images.to(device, non_blocking=True)
        logits = model(images)
        probs = F.softmax(logits, dim=1)
        all_probs.append(probs.cpu())
        all_preds.append(probs.argmax(dim=1).cpu())
        all_targets.append(targets)
    return (
        torch.cat(all_preds).numpy(),
        torch.cat(all_targets).numpy(),
        torch.cat(all_probs).numpy(),
    )


def build_metrics_table(y_true, y_pred, class_names) -> pd.DataFrame:
    """Таблица метрик по классам + агрегаты (macro / weighted avg)."""
    report = classification_report(
        y_true, y_pred, target_names=class_names, output_dict=True, zero_division=0
    )
    df = pd.DataFrame(report).T
    df = df.rename(columns={"support": "n_samples"})
    df["n_samples"] = df["n_samples"].astype(int)
    return df.round(4)


def main():
    parser = argparse.ArgumentParser(description="Обучение CNN на CIFAR-10")
    parser.add_argument("--data-dir", type=str, default="./data")
    parser.add_argument("--out-dir", type=str, default="./results")
    parser.add_argument("--epochs", type=int, default=20)
    parser.add_argument("--batch-size", type=int, default=128)
    parser.add_argument("--lr", type=float, default=1e-3)
    parser.add_argument("--weight-decay", type=float, default=5e-4)
    parser.add_argument("--patience", type=int, default=7, help="эпохи без улучшения до остановки")
    parser.add_argument("--num-workers", type=int, default=2)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    set_seed(args.seed)
    device = get_device()
    print(f"Устройство: {device}")

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------ #
    # Данные: скачиваем CIFAR-10 и делим train на train/val.
    # Train — с аугментациями, val/test — только нормализация.
    # ------------------------------------------------------------------ #
    train_tf = transforms.Compose([
        transforms.RandomCrop(32, padding=4),
        transforms.RandomHorizontalFlip(),
        transforms.ToTensor(),
        transforms.Normalize(CIFAR_MEAN, CIFAR_STD),
    ])
    eval_tf = transforms.Compose([
        transforms.ToTensor(),
        transforms.Normalize(CIFAR_MEAN, CIFAR_STD),
    ])

    full_train = datasets.CIFAR10(args.data_dir, train=True, download=True, transform=train_tf)
    # Отдельный экземпляр с eval-трансформами для валидации,
    # чтобы на val не применялись аугментации.
    full_train_eval = datasets.CIFAR10(args.data_dir, train=True, download=False, transform=eval_tf)
    test_ds = datasets.CIFAR10(args.data_dir, train=False, download=True, transform=eval_tf)

    val_fraction = 0.1
    n_total = len(full_train)
    n_val = int(n_total * val_fraction)
    n_train = n_total - n_val

    gen = torch.Generator().manual_seed(args.seed)
    train_ds, _ = random_split(full_train, [n_train, n_val], generator=gen)
    # Тот же генератор => то же разбиение индексов
    gen = torch.Generator().manual_seed(args.seed)
    _, val_ds = random_split(full_train_eval, [n_train, n_val], generator=gen)

    # ------------------------------------------------------------------ #
    # DataLoader'ы. pin_memory ускоряет копирование на GPU.
    # ------------------------------------------------------------------ #
    common = dict(num_workers=args.num_workers, pin_memory=torch.cuda.is_available())
    train_loader = DataLoader(train_ds, batch_size=args.batch_size, shuffle=True, drop_last=True, **common)
    val_loader = DataLoader(val_ds, batch_size=args.batch_size * 2, shuffle=False, **common)
    test_loader = DataLoader(test_ds, batch_size=args.batch_size * 2, shuffle=False, **common)

    # --- Модель ---
    model = SimpleCNN(num_classes=len(CLASS_NAMES)).to(device)
    n_params = sum(p.numel() for p in model.parameters() if p.requires_grad)

    # --- Обучение ---
    ckpt_path = out_dir / "best_model.pt"
    model, history_df, best_val_acc = train(
        model, train_loader, val_loader, device,
        epochs=args.epochs, lr=args.lr, weight_decay=args.weight_decay,
        patience=args.patience, ckpt_path=ckpt_path,
    )
    history_df.to_csv(out_dir / "training_history.csv", index=False)

    # --- Постобработка: тест ---
    y_pred, y_true, y_probs = predict(model, test_loader, device)

    test_acc = accuracy_score(y_true, y_pred)
    macro_f1 = f1_score(y_true, y_pred, average="macro")

    # Confusion matrix -> CSV
    cm = confusion_matrix(y_true, y_pred)
    cm_df = pd.DataFrame(cm, index=CLASS_NAMES, columns=CLASS_NAMES)
    cm_df.to_csv(out_dir / "confusion_matrix.csv")

    # Вероятности предсказаний -> npz (пригодится для анализа ошибок / калибровки)
    np.savez_compressed(out_dir / "test_predictions.npz",
                        y_true=y_true, y_pred=y_pred, y_probs=y_probs)

    # --- Таблица метрик ---
    metrics_df = build_metrics_table(y_true, y_pred, CLASS_NAMES)
    metrics_df.to_csv(out_dir / "metrics_table.csv")