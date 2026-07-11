"""The model registry and the champion alias (plan, phase 6; Notations.md §17).

The registry is the list of trained model versions that MLflow keeps in the
local store ``data/ml/mlflow/``. One registered model, ``demand-model``,
holds every version regardless of family. A model version is a fitted model
plus what defines it — three tags: ``model_family``, ``train_months``, and
``data_version``. Its files are the folder the model's ``save`` wrote,
logged to a training run in the ``demand-training`` experiment.

The alias ``champion`` marks the version the platform currently uses. The
forecast builder (``gbp/ml/forecast.py``) resolves the model through
:func:`resolve_champion` — never by a file path. Promotion moves the alias
(:func:`promote_to_champion`); the version it left keeps the tag
``role: challenger``.

The retraining pipeline (``gbp/ml/pipeline.py``) is the only writer: it
registers a candidate version (:func:`register_version`, reusing an existing
twin through :func:`find_version`) and promotes or keeps the champion by the
backtest comparison.
"""

from __future__ import annotations

import pathlib
import tempfile

import mlflow
from mlflow.entities.model_registry import ModelVersion
from mlflow.exceptions import MlflowException
from mlflow.tracking import MlflowClient

from gbp.ml.data import ml_dir, normalize_month
from gbp.ml.models import DemandModel, load_model

#: The one registered model every version belongs to.
REGISTERED_MODEL_NAME = "demand-model"

#: The movable alias that points at the version the platform uses.
CHAMPION_ALIAS = "champion"

#: The experiment the training runs (one per registered version) log into.
TRAINING_EXPERIMENT = "demand-training"


def configure_mlflow(tracking_dir: pathlib.Path | None = None) -> pathlib.Path:
    """Point MLflow at the local store and return the store folder.

    The store is one folder (default ``data/ml/mlflow/``): the runs and the
    registry in ``mlflow.db`` (SQLite), the logged files under
    ``artifacts/``. Every function in this module and the backtest call this
    first, so the whole platform reads and writes one store.
    """
    store = (tracking_dir or (ml_dir() / "mlflow")).resolve()
    store.mkdir(parents=True, exist_ok=True)
    mlflow.set_tracking_uri(f"sqlite:///{store / 'mlflow.db'}")
    return store


def ensure_experiment(name: str, store: pathlib.Path) -> None:
    """Create the experiment with its files inside the store, then select it.

    MLflow 3 no longer accepts a plain directory as a store, so the
    experiment's artifact location is set explicitly to ``<store>/artifacts``.
    """
    if mlflow.get_experiment_by_name(name) is None:
        mlflow.create_experiment(name, artifact_location=(store / "artifacts").as_uri())
    mlflow.set_experiment(name)


def version_tags(family: str, train_months: list[str], data_version: str) -> dict[str, str]:
    """Build the three tags that define a model version — its identity in the registry."""
    months = sorted(normalize_month(m) for m in train_months)
    return {
        "model_family": family,
        "train_months": ",".join(months),
        "data_version": data_version,
    }


def find_version(
    family: str,
    train_months: list[str],
    data_version: str,
    tracking_dir: pathlib.Path | None = None,
) -> ModelVersion | None:
    """Return the registered version with the same identity tags, or None.

    This is the idempotency check of the pipeline's train step: the same
    family trained on the same months of the same data version is the same
    model, so it is reused instead of registered twice.
    """
    configure_mlflow(tracking_dir)
    client = MlflowClient()
    wanted = version_tags(family, train_months, data_version)
    versions = client.search_model_versions(f"name = '{REGISTERED_MODEL_NAME}'")
    matches = [v for v in versions if {key: v.tags.get(key) for key in wanted} == wanted]
    return max(matches, key=lambda v: int(v.version), default=None)


def register_version(
    model: DemandModel,
    *,
    train_months: list[str],
    data_version: str,
    tracking_dir: pathlib.Path | None = None,
) -> ModelVersion:
    """Save a fitted model as a new version of ``demand-model``.

    Logs one training run (parameters: family, months, data version, the
    model's own settings; files: the folder ``save`` wrote, under ``model/``)
    and registers that run's files as a new version with the identity tags.
    The new version carries no alias — promotion is the pipeline's decision,
    not the trainer's.
    """
    store = configure_mlflow(tracking_dir)
    ensure_experiment(TRAINING_EXPERIMENT, store)
    months = sorted(normalize_month(m) for m in train_months)
    span = f"{months[0]}..{months[-1]}"
    with mlflow.start_run(run_name=f"train-{model.name}-{months[-1]}") as run:
        mlflow.log_params(
            {
                "model": model.name,
                "train_months": span,
                "data_version": data_version,
                **model.params(),
            }
        )
        with tempfile.TemporaryDirectory() as folder:
            model.save(pathlib.Path(folder))
            mlflow.log_artifacts(folder, artifact_path="model")

    client = MlflowClient()
    try:
        client.create_registered_model(
            REGISTERED_MODEL_NAME,
            description=(
                "Demand forecasting models behind the one interface; "
                f"the {CHAMPION_ALIAS!r} alias marks the version the platform uses."
            ),
        )
    except MlflowException as error:
        if error.error_code != "RESOURCE_ALREADY_EXISTS":
            raise
    return client.create_model_version(
        REGISTERED_MODEL_NAME,
        source=f"{run.info.artifact_uri}/model",
        run_id=run.info.run_id,
        tags=version_tags(model.name, months, data_version),
        description=f"{model.name} trained on {span} (data version {data_version})",
    )


def latest_registered_version(
    tracking_dir: pathlib.Path | None = None,
) -> ModelVersion | None:
    """Return the newest registered version, or None while the registry is empty.

    The pipeline's promote step uses this as the candidate when it runs
    without a train step in the same command.
    """
    configure_mlflow(tracking_dir)
    versions = MlflowClient().search_model_versions(f"name = '{REGISTERED_MODEL_NAME}'")
    return max(versions, key=lambda v: int(v.version), default=None)


def champion_version(tracking_dir: pathlib.Path | None = None) -> ModelVersion | None:
    """Return the version the champion alias points at, or None before the first promotion."""
    configure_mlflow(tracking_dir)
    client = MlflowClient()
    try:
        return client.get_model_version_by_alias(REGISTERED_MODEL_NAME, CHAMPION_ALIAS)
    except MlflowException as error:
        if error.error_code in ("RESOURCE_DOES_NOT_EXIST", "INVALID_PARAMETER_VALUE"):
            return None
        raise


def promote_to_champion(version: ModelVersion, tracking_dir: pathlib.Path | None = None) -> None:
    """Move the champion alias to ``version``; the old champion becomes a challenger."""
    configure_mlflow(tracking_dir)
    client = MlflowClient()
    previous = champion_version(tracking_dir)
    # The store returns version numbers as int or str depending on the
    # backend; compare them as strings.
    if previous is not None and str(previous.version) != str(version.version):
        client.set_model_version_tag(REGISTERED_MODEL_NAME, previous.version, "role", "challenger")
    client.set_registered_model_alias(REGISTERED_MODEL_NAME, CHAMPION_ALIAS, version.version)
    client.set_model_version_tag(REGISTERED_MODEL_NAME, version.version, "role", "champion")


def mark_challenger(version: ModelVersion, tracking_dir: pathlib.Path | None = None) -> None:
    """Tag a version that lost the comparison; it waits for the next one."""
    configure_mlflow(tracking_dir)
    MlflowClient().set_model_version_tag(
        REGISTERED_MODEL_NAME, version.version, "role", "challenger"
    )


def load_version_model(
    version: ModelVersion, tracking_dir: pathlib.Path | None = None
) -> DemandModel:
    """Download a version's files and load the fitted model behind the interface.

    The family comes from the version's ``model_family`` tag; the files go
    to a temporary folder that is deleted once the model is in memory.
    """
    configure_mlflow(tracking_dir)
    family = version.tags.get("model_family")
    if not family:
        raise ValueError(
            f"version {version.version} of {REGISTERED_MODEL_NAME} has no "
            "model_family tag; it was not registered by register_version"
        )
    with tempfile.TemporaryDirectory() as folder:
        local = mlflow.artifacts.download_artifacts(
            f"models:/{REGISTERED_MODEL_NAME}/{version.version}", dst_path=folder
        )
        return load_model(family, pathlib.Path(local))


def resolve_champion(
    tracking_dir: pathlib.Path | None = None,
) -> tuple[DemandModel, ModelVersion]:
    """Return the champion, loaded and ready to predict, with its version.

    Raises ``LookupError`` when the registry has no champion yet — the
    retraining pipeline promotes the first one.
    """
    version = champion_version(tracking_dir)
    if version is None:
        raise LookupError(
            "the registry has no champion yet; run the retraining pipeline "
            "first (python -m gbp.ml.pipeline)"
        )
    return load_version_model(version, tracking_dir), version
