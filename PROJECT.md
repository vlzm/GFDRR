# Graph-Based Logistics Platform

## Vision

Платформа для моделирования и оптимизации логистических сетей. Ядро — **Environment**: пространство, в котором commodity (велосипеды, товары, деньги) перемещаются через сеть объектов по периодам времени. Внутри Environment работают задачи (ребалансировка, ремонт, диспатч), принимаются решения, обновляется состояние мира.

Модель данных — domain-agnostic, построена на multi-commodity flow формулировке (Williamson, Ahuja/Magnanti/Orlin). Табличные структуры для pandas/PySpark. Параметры — через `AttributeRegistry` с кастомными grain'ами.

Первый домен — велошеринг (Citi Bike-style).

### Два уровня задач

**Операционный (Environment)** — пошаговая симуляция. Environment идёт по периодам: period 0 → 1 → 2... На каждом шаге: происходят поездки, обновляется inventory, запускаются задачи (ребалансировщик ночью, ремонт утром). Состояние мира меняется после каждого шага. Это digital twin: "что происходит каждый день".

**Стратегический (Optimizer)** — решение "за один раз". Берёт данные за год (все потенциальные поездки, все стоимости), формулирует LP/MILP, солвер минимизирует cost function. Нет пошагового процесса — все периоды видны сразу. Это стратегическое планирование: "сколько грузовиков купить, где разместить депо".

Оба уровня используют один и тот же `ResolvedModelData`, но обрабатывают его по-разному.

---

## Roadmap

1. **Foundation** — модель данных, build pipeline, loader
2. **Environment** — step-by-step engine, state management
3. **Rebalancer** — первая задача внутри Environment (VRP)
4. **Trip Generator** — синтетический поток поездок для симуляции
5. **UI** — визуализация Environment (Streamlit)
6. **Infrastructure** — DB, API, Docker, CI/CD
7. **Cloud** — Azure deployment

Каждая фаза начинается с design doc. Текущий прогресс — в `PROJECT_STATE.md`.

---

## Принципы

**Minimalism (Nano-style).** Код должен быть hackable. Без model factories, тяжёлых DI-контейнеров, скрытой магии. Каждый файл можно понять за 5 минут.

**Vectorization first.** Вся математика через pandas/NumPy. Никаких `for` циклов по данным в hot paths.

**Design doc before code.** Каждая новая подсистема начинается с design doc, обсуждения, и только потом — реализация по milestone'ам.

**Структурное vs параметрическое.** Topology (facilities, edges, demand) — фиксированные поля. Числовые параметры (costs, capacities) — через AttributeRegistry с кастомными grain'ами.

**Strict typing.** Pydantic для всех контрактов. Type hints на всех public функциях.

**English in code, Russian in chat.** Код, комментарии, docstrings — только на английском. Общение с пользователем — на русском.

---

## Ключевые документы

| Документ | Назначение | Частота обновления |
|----------|------------|--------------------|
| `PROJECT.md` | Vision, roadmap, принципы | Редко (при смене vision) |
| `PROJECT_STATE.md` | Текущая фаза, прогресс, "not now" | При переходе между фазами и внутри фазы |
| `.cursorrules` | Правила для AI, инварианты модели | Редко |
| `docs/design/` | Design docs по подсистемам | По одному на фазу |
| `docs/IDEAS.md` | Парковка идей (ML, GNN, ...) | Когда появляется идея |
