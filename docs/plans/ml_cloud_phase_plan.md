# ML cloud phase — decisions made in advance

This document records the decisions about the Azure cloud phase of the ML
demand forecast work, so they are not lost by the time that phase starts.
The local phases come first and are described in
[ml_demand_forecast_plan.md](ml_demand_forecast_plan.md). Do not start the
cloud phase before the local phases 1–7 work.

The goal of the cloud phase: the retraining pipeline, data versioning, and
monitoring from the local plan run in Azure on a schedule, at near-zero cost,
inside a fixed time budget of 3–5 days.

## The main decision: minimal Azure services, not Databricks

The cloud phase uses a small set of cheap services and deliberately does not
use Databricks. This is not a shortcut; at this scale it is the correct
production choice, and the reasoning is part of what the project demonstrates.

The scale facts that drive the choice:

- Training data: a few gigabytes of monthly trip files.
- Training job: one LightGBM fit plus baselines, minutes of compute.
- Cadence: once a month, when Citi Bike publishes a new file.
- Consumers: about ten users reading batch forecasts from disk.

At this scale a Spark cluster, Unity Catalog, and a managed orchestrator add
cost and moving parts without adding capability. The project documentation
must state this reasoning explicitly, together with the threshold at which the
choice would flip (see "What changes at scale" below). A cost-aware,
scale-appropriate choice with a written justification is a stronger
demonstration of engineering judgment than using the heaviest tool available.

## The service set

| Need | Azure service | Expected cost |
|---|---|---|
| Data and artifacts (`data/raw/`, training tables, forecasts, monitoring files) | Blob Storage | cents per month for a few GB |
| Scheduled retraining | Container Apps Jobs with the built-in cron trigger | near zero: the free grant is 180,000 vCPU-seconds per month; the job needs minutes once a month |
| Experiment tracking and model registry | Azure ML workspace used as a managed MLflow server | the workspace itself is free; only compute and storage are billed, and the job runs its own compute |

Total expected cost: under five dollars per month, likely near zero.

Note on names: the storage service is Blob Storage. S3 is the AWS service;
the two must not be confused in documents or code comments.

Rejected options, and why:

- **Databricks** — see the main decision above.
- **Logic App / Power Automate as the scheduler** — Container Apps Jobs has a
  cron trigger built in, so a separate scheduling service adds nothing.
  Power Automate is an office automation tool and does not belong in an ML
  pipeline.
- **Two account types in the Streamlit app (user vs. extended)** — not
  needed. The app is an internal operator tool, not a consumer product. A
  model monitoring page in an operator tool is normal, and phase 7 of the
  local plan already includes it. If separation is ever wanted, a collapsible
  sidebar section is enough; do not build authentication or roles.

## Docker enters at this phase, deliberately

The project rule "do not build Docker" applies to the local platform phases.
The cloud phase needs exactly one container image, because Container Apps
Jobs runs containers. Building that one image is a deliberate scope change
that starts with this phase, and `CLAUDE.md` should be updated to say so when
the phase begins.

## Why the migration is small

The local plan was designed so that this phase is a thin wrapper, not a
rewrite:

- The pipeline steps (`download → build-table → train → backtest → promote`)
  are idempotent CLI commands. The cloud job runs the same commands.
- The trigger logic lives in the pipeline, not in cron. The cloud scheduler
  only decides *when* to run; it contains no logic.
- The forecast builder resolves the model through the MLflow registry alias
  `champion`, never through a file path. Pointing MLflow at the Azure ML
  workspace changes a URI, not the code.

## Scope and time budget

The phase is limited to 3–5 days and to this list:

1. Data and artifacts move to Blob Storage.
2. One container image that runs the existing pipeline command.
3. One Container Apps Job with a monthly cron trigger.
4. MLflow tracking and registry pointed at the Azure ML workspace.
5. A short architecture document with a diagram and the decision log.

Everything that does not fit goes into the architecture document as a written
design, not into code. Known examples: an Event Grid trigger on the arrival
of a new monthly file, Key Vault for secrets, Unity Catalog for data and
model versioning.

## What changes at scale

The architecture document must contain a section that answers: at what point
would the minimal setup stop being right, and what would replace it. The
expected content:

- When data grows past what a single machine handles in reasonable time, or
  retraining becomes daily, move training to Databricks jobs.
- When several teams share the data and models, replace DVC and the local
  conventions with Unity Catalog.
- When consumers need forecasts on request rather than monthly batches, add
  an online serving endpoint (the local plan already names `app/api.py` as
  the place).

## How to present the result

The value of this project at an interview comes from three artifacts, not
from the number of cloud services:

1. The architecture document with the decision log: why batch and not an
   online service, why one global model, why not Databricks and when the
   answer would become yes.
2. The pipeline log table from phase 6 of the local plan: one row per run —
   when, which data version, which model version, promoted or not, and why.
   It is direct evidence that promotion is a rule, not a manual choice.
3. The two-level evaluation through the simulator (phase 5): a model is good
   when it leads to the same decisions, not only when its error is small.
   This is the part no standard portfolio project has.

The project maps one-to-one onto the standard ML system design interview
structure: requirements → data → features → training → evaluation →
deployment → monitoring → retraining. When the local system works, rehearse
telling the story in exactly that order once.
