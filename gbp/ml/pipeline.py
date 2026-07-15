"""The retraining pipeline (plan, phase 6; Notations.md §17).

Five idempotent steps, runnable as one command and one by one::

    download → build-table → train → backtest → promote

- ``download`` — the trigger lives here: ask the bucket for published months
  missing from ``data/raw/`` and fetch them (trips plus status dumps). A
  scheduler (cron, later a cloud job) only has to start the pipeline.
- ``build-table`` — build the missing training partitions, oldest first.
- ``train`` — fit the candidate family on every partition on disk and
  register it as a model version (Notations.md §17). The same family on the
  same months of the same data version is the same model — the existing
  version is reused, not registered twice.
- ``backtest`` — the phase-4 backtest over the candidate's family and the
  champion's family — the same splits for both; the backtest itself scores
  every split against the seasonal naive baseline.
- ``promote`` — the rule, not a manual choice: the candidate becomes
  champion only when its mean backtest score is at least as good as the
  champion's over the same splits (equal promotes — same recipe, newer data
  version wins). Otherwise it stays a challenger. Either way one row goes to
  the pipeline log ``data/ml/pipeline_log.csv``: when, which data version,
  which versions with their scores, promoted or not, and why.

Rerunning the whole pipeline with no new data changes nothing: nothing to
download, nothing to build, the train step reuses the registered version,
and when that version is already the champion the backtest is skipped and
the log row says so.

Terminal use::

    python -m gbp.ml.pipeline                       # all steps, lightgbm
    python -m gbp.ml.pipeline --steps train backtest promote
    python -m gbp.ml.pipeline --months 202602       # bound download/build-table
"""

from __future__ import annotations

import argparse
import datetime
import pathlib
import subprocess
from collections.abc import Callable, Sequence

import pandas as pd
from mlflow.entities.model_registry import ModelVersion

from gbp.loaders.download import (
    download_months,
    month_zip_keys,
    normalize_month,
    raw_trip_months,
)
from gbp.ml.backtest import data_version, run_backtest
from gbp.ml.data import MlPaths, ml_dir
from gbp.ml.models import MODEL_FAMILIES, create_model
from gbp.ml.registry import MlflowStore
from gbp.ml.station_status import download_status_months, next_month
from gbp.ml.training import (
    load_training_table,
    partition_path,
    training_dir,
    write_month_partition,
)

#: The steps, in the only order they run in.
STEPS = ("download", "build-table", "train", "backtest", "promote")

#: The backtest metric the promote rule compares (mean over the splits).
PRIMARY_METRIC = "mae"

#: The columns of one pipeline-log row.
LOG_COLUMNS = [
    "run_at",
    "data_version",
    "model_family",
    "train_months",
    "candidate_version",
    "champion_version",
    "candidate_mae",
    "champion_mae",
    "promoted",
    "reason",
]


def pipeline_log_path() -> pathlib.Path:
    """Where the pipeline log lives: ``<ml dir>/pipeline_log.csv``."""
    return ml_dir() / "pipeline_log.csv"


def append_log_row(row: dict[str, object], path: pathlib.Path | None = None) -> pathlib.Path:
    """Append one decision row to the pipeline log, creating the file if needed."""
    path = path or pipeline_log_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    frame = pd.DataFrame([{column: row.get(column) for column in LOG_COLUMNS}])
    frame.to_csv(path, mode="a", header=not path.exists(), index=False)
    return path


def partition_months(training_root: pathlib.Path | None = None) -> list[str]:
    """Return the months whose training partitions are on disk, sorted."""
    return sorted(p.stem for p in (training_root or training_dir()).glob("*.parquet"))


def published_missing_months(
    known_months: Sequence[str], today: pd.Timestamp | None = None
) -> list[str]:
    """Return the months the bucket has published but the disk does not have — the trigger.

    Checks each calendar month after the newest known one, up to the current
    month. Citi Bike publishes with about a month's delay and in order, so
    the scan stops at the first unpublished month; most days this returns
    nothing or one month.
    """
    if not known_months:
        return []
    last_known = pd.Period(max(normalize_month(m) for m in known_months), freq="M")
    current = pd.Period(today or pd.Timestamp.today(), freq="M")
    published: list[str] = []
    for period in pd.period_range(last_known + 1, current, freq="M"):
        month = period.strftime("%Y%m")
        try:
            month_zip_keys(month)
        except ValueError:
            break
        published.append(month)
    return published


def refresh_dvc(log: Callable[[str], None] = print) -> None:
    """Update the ``.dvc`` files after the tracked data changed.

    Runs ``dvc add`` on ``data/raw`` and ``data/ml/training`` so the data
    version (the git commit of the ``.dvc`` files, ``-dirty`` while they are
    uncommitted) reflects the new months. Without dvc on PATH, or on any dvc
    error, the pipeline continues with a note — the log then names the stale
    version, which is still better than stopping a retrain.
    """
    repo = pathlib.Path(__file__).resolve().parents[2]
    try:
        subprocess.run(
            ["dvc", "add", "data/raw", "data/ml/training"],
            cwd=repo,
            check=True,
            capture_output=True,
            text=True,
        )
        log("dvc: .dvc files updated (commit them to pin the data version)")
    except (OSError, subprocess.CalledProcessError) as error:
        log(f"dvc add skipped: {error}")


def step_download(
    months: Sequence[str] | None = None,
    raw: pathlib.Path | None = None,
    log: Callable[[str], None] = print,
) -> list[str]:
    """Fetch the trigger months (or the given ones): trip CSVs plus status dumps.

    Without ``months``, asks the bucket for published months missing from
    disk. Months already on disk are skipped by the downloaders, so the step
    is idempotent. Returns the months that brought new files.
    """
    if months is None:
        known = raw_trip_months(raw)
        if not known:
            raise FileNotFoundError(
                "data/raw/ has no trip CSVs; give the starting months explicitly (--months)"
            )
        months = published_missing_months(known)
        if not months:
            log("download: no new months published; disk is current")
            return []
    months = sorted(normalize_month(m) for m in months)
    new_files = download_months(months, raw, log)
    status_months = sorted({m for month in months for m in (month, next_month(month))})
    download_status_months(status_months, raw, log)
    return sorted({m for m in months for f in new_files if f"{m}-citibike-tripdata" in f.name})


def step_build_table(
    months: Sequence[str] | None = None,
    paths: MlPaths | None = None,
    log: Callable[[str], None] = print,
) -> list[str]:
    """Build the missing training partitions, oldest first. Returns the built months.

    Without ``months``, builds every month whose CSVs are on disk but whose
    partition is not. A partition already on disk is kept as is.
    """
    paths = paths or MlPaths.resolve()
    if months is None:
        months = raw_trip_months(paths.raw)
    built: list[str] = []
    for month in sorted(normalize_month(m) for m in months):
        path = partition_path(month, paths.training)
        if path.exists():
            log(f"build-table {month}: partition exists, skipping")
            continue
        write_month_partition(month, paths.raw, paths.training)
        log(f"build-table {month}: -> {path}")
        built.append(month)
    if not built:
        log("build-table: all partitions on disk")
    return built


def step_train(
    family: str = "lightgbm",
    months: Sequence[str] | None = None,
    *,
    data_ver: str | None = None,
    training_root: pathlib.Path | None = None,
    store: MlflowStore | None = None,
    log: Callable[[str], None] = print,
) -> ModelVersion:
    """Fit the candidate on every partition and register it, reusing a twin.

    The idempotent step: a registered version with the same family, months,
    and data version is returned as is — fitting again would only register a
    twin of it.
    """
    store = store or MlflowStore()
    if months is None:
        months = partition_months(training_root)
    if not months:
        raise FileNotFoundError("no training partitions; run the build-table step first")
    months = sorted(normalize_month(m) for m in months)
    span = f"{months[0]}..{months[-1]}"
    version = data_ver or data_version()
    existing = store.find_version(family, list(months), version)
    if existing is not None:
        log(
            f"train: version {existing.version} already holds {family} on {span} "
            f"(data {version}); reusing it"
        )
        return existing
    log(f"train: fitting {family} on {span} (data {version}) ...")
    table = load_training_table(list(months), training_root)
    model = create_model(family)
    model.fit(table)
    registered = store.register_version(model, train_months=list(months), data_version=version)
    log(f"train: registered version {registered.version}")
    return registered


def step_backtest(
    candidate: ModelVersion,
    months: Sequence[str] | None = None,
    n_splits: int = 3,
    *,
    paths: MlPaths | None = None,
    store: MlflowStore | None = None,
    weather_df: pd.DataFrame | None = None,
    log: Callable[[str], None] = print,
) -> pd.DataFrame:
    """Backtest the candidate's and the champion's families over the same splits.

    The phase-4 backtest, unchanged: the promote rule needs both sides
    scored on the same splits. The backtest itself scores every split
    against the shared naive month forecast, so the seasonal naive does not
    need to run as a family here. When the champion is the same family as
    the candidate (or there is no champion yet), one set of scores serves
    both sides.
    """
    paths = paths or MlPaths.resolve()
    store = store or MlflowStore()
    if months is None:
        months = partition_months(paths.training)
    families = [str(candidate.tags["model_family"])]
    champion = store.champion_version()
    if champion is not None and champion.tags["model_family"] not in families:
        families.append(str(champion.tags["model_family"]))
    return run_backtest(
        list(months),
        families,
        n_splits,
        paths=paths,
        store=store,
        weather_df=weather_df,
        log=log,
    )


def family_score(comparison: pd.DataFrame, family: str) -> float:
    """Read the family's mean ``PRIMARY_METRIC`` over the splits off the comparison table."""
    table = comparison.set_index("model")
    if family not in table.index:
        raise ValueError(
            f"the comparison has no {family!r} row; rerun the backtest step with it included"
        )
    return float(table.loc[family, PRIMARY_METRIC])


def promote_decision(
    candidate: ModelVersion,
    champion: ModelVersion | None,
    comparison: pd.DataFrame | None,
) -> tuple[bool, str, float | None, float | None]:
    """Apply the promote rule; return (promoted, reason, candidate score, champion score).

    The rule: no champion → the candidate is promoted; the candidate already
    is the champion → nothing changes; otherwise the candidate must score at
    least as well as the champion on ``PRIMARY_METRIC`` over the same splits.
    Equal scores promote — the two sides are then the same recipe refit on
    the same splits, and the newer data version wins the tie.
    """
    if champion is None:
        score = (
            family_score(comparison, str(candidate.tags["model_family"]))
            if comparison is not None
            else None
        )
        return True, "no champion yet; the first registered version is promoted", score, None
    if str(champion.version) == str(candidate.version):
        return (
            False,
            f"version {candidate.version} is already the champion; nothing to compare",
            None,
            None,
        )
    if comparison is None:
        raise ValueError(
            "a champion exists but there is no backtest comparison for the current "
            "data; run the backtest step first"
        )
    candidate_score = family_score(comparison, str(candidate.tags["model_family"]))
    champion_score = family_score(comparison, str(champion.tags["model_family"]))
    if candidate_score <= champion_score:
        return (
            True,
            f"candidate {PRIMARY_METRIC} {candidate_score:.4f} <= champion "
            f"{champion_score:.4f} over the same splits",
            candidate_score,
            champion_score,
        )
    return (
        False,
        f"candidate {PRIMARY_METRIC} {candidate_score:.4f} > champion "
        f"{champion_score:.4f} over the same splits; kept as challenger",
        candidate_score,
        champion_score,
    )


def step_promote(
    candidate: ModelVersion,
    comparison: pd.DataFrame | None,
    *,
    data_ver: str | None = None,
    store: MlflowStore | None = None,
    log_path: pathlib.Path | None = None,
    log: Callable[[str], None] = print,
) -> dict[str, object]:
    """Promote or keep the champion by the rule, and append the log row.

    Returns the row that went to the pipeline log.
    """
    store = store or MlflowStore()
    champion = store.champion_version()
    promoted, reason, candidate_score, champion_score = promote_decision(
        candidate, champion, comparison
    )
    if promoted:
        store.promote_to_champion(candidate)
    elif champion is not None and str(champion.version) != str(candidate.version):
        store.mark_challenger(candidate)
    row: dict[str, object] = {
        "run_at": datetime.datetime.now().isoformat(timespec="seconds"),
        "data_version": data_ver or data_version(),
        "model_family": candidate.tags.get("model_family"),
        "train_months": candidate.tags.get("train_months"),
        "candidate_version": str(candidate.version),
        "champion_version": None if champion is None else str(champion.version),
        "candidate_mae": candidate_score,
        "champion_mae": champion_score,
        "promoted": promoted,
        "reason": reason,
    }
    path = append_log_row(row, log_path)
    log(f"promote: {'promoted' if promoted else 'champion kept'} — {reason}")
    log(f"promote: logged to {path}")
    return row


def run_pipeline(
    steps: Sequence[str] = STEPS,
    *,
    family: str = "lightgbm",
    months: Sequence[str] | None = None,
    n_splits: int = 3,
    paths: MlPaths | None = None,
    log_path: pathlib.Path | None = None,
    weather_df: pd.DataFrame | None = None,
    log: Callable[[str], None] = print,
) -> dict[str, object] | None:
    """Run the requested steps in their fixed order; return the promote row if promote ran.

    ``months`` bounds only the download and build-table steps; training and
    the backtest always read every partition on disk — a new month extends
    the history, it does not replace it. The ``.dvc`` files are refreshed
    when download or build-table changed the default data folders, so the
    data version in the log names what was actually trained on — a run with
    its own ``paths`` (a test on ``tmp_path``) never touches dvc. ``paths``
    also names the MLflow store folder (``paths.tracking``); the store object
    is created once here and passed to every step that talks to MLflow.
    """
    unknown = set(steps) - set(STEPS)
    if unknown:
        raise ValueError(f"unknown steps: {', '.join(sorted(unknown))}; known: {', '.join(STEPS)}")
    ordered = [step for step in STEPS if step in steps]

    on_default_folders = paths is None
    paths = paths or MlPaths.resolve()

    changed = False
    if "download" in ordered:
        changed |= bool(step_download(months, paths.raw, log))
    if "build-table" in ordered:
        changed |= bool(step_build_table(months, paths, log))
    if changed and on_default_folders:
        refresh_dvc(log)

    if not ({"train", "backtest", "promote"} & set(ordered)):
        return None
    version = data_version()
    store = MlflowStore(paths.tracking)

    if "train" in ordered:
        candidate = step_train(
            family,
            data_ver=version,
            training_root=paths.training,
            store=store,
            log=log,
        )
    else:
        found = store.latest_registered_version()
        if found is None:
            raise LookupError("the registry has no versions; run the train step first")
        candidate = found

    champion = store.champion_version()
    already_champion = champion is not None and str(champion.version) == str(candidate.version)

    comparison: pd.DataFrame | None = None
    if "backtest" in ordered and not already_champion:
        comparison = step_backtest(
            candidate,
            n_splits=n_splits,
            paths=paths,
            store=store,
            weather_df=weather_df,
            log=log,
        )
    elif "backtest" in ordered:
        log("backtest: candidate is already the champion; skipping")

    if "promote" not in ordered:
        return None
    if comparison is None and champion is not None and not already_champion:
        saved = store.latest_comparison()
        if saved is not None:
            if saved.data_version == version:
                comparison = saved.table
            else:
                log(
                    f"promote: the newest comparison is for data "
                    f"{saved.data_version}, not {version}; ignoring it"
                )
    return step_promote(
        candidate,
        comparison,
        data_ver=version,
        store=store,
        log_path=log_path,
        log=log,
    )


def main() -> None:
    """Terminal entry point: one command from new raw data to a promote-or-keep decision."""
    parser = argparse.ArgumentParser(
        description="Retraining pipeline: download -> build-table -> train -> backtest -> promote."
    )
    parser.add_argument(
        "--steps",
        nargs="+",
        choices=list(STEPS),
        default=list(STEPS),
        help="steps to run (default: all, in their fixed order)",
    )
    parser.add_argument(
        "--model",
        default="lightgbm",
        choices=list(MODEL_FAMILIES),
        help="candidate model family (default: lightgbm)",
    )
    parser.add_argument(
        "--months",
        nargs="*",
        default=None,
        help="months for download/build-table, as YYYYMM or YYYY-MM "
        "(default: ask the bucket for new months)",
    )
    parser.add_argument(
        "--splits", type=int, default=3, help="held-out months in the backtest (default: 3)"
    )
    args = parser.parse_args()

    row = run_pipeline(
        args.steps,
        family=args.model,
        months=args.months,
        n_splits=args.splits,
    )
    if row is not None:
        print()
        decision = "PROMOTED" if row["promoted"] else "CHAMPION KEPT"
        print(f"{decision}: {row['reason']}")


if __name__ == "__main__":
    main()
