"""The model registry and the champion alias, backed by a local MLflow store."""

from __future__ import annotations

import dataclasses
import pathlib
import tempfile
import typing

import mlflow
import pandas as pd
from mlflow.entities.model_registry import ModelVersion
from mlflow.exceptions import MlflowException
from mlflow.tracking import MlflowClient

from gbp.loaders.download import normalize_month
from gbp.ml.data import ml_dir
from gbp.ml.models import DemandModel, load_model

#: The one registered model every version belongs to.
REGISTERED_MODEL_NAME = "demand-model"

#: The movable alias that points at the version the platform uses.
CHAMPION_ALIAS = "champion"

#: The experiment the training runs (one per registered version) log into.
TRAINING_EXPERIMENT = "demand-training"

#: The experiment the backtest runs (per split and per model) log into. The
#: cross-model comparison is one more run in it, stored by the two methods
#: below.
BACKTEST_EXPERIMENT = "demand-backtest"

#: How a saved comparison is found inside the backtest experiment: the run
#: name and the CSV file name. Only :meth:`MlflowStore.log_comparison` and
#: :meth:`MlflowStore.latest_comparison` know them.
_COMPARISON_RUN_NAME = "comparison"
_COMPARISON_FILE = "comparison.csv"


@dataclasses.dataclass(frozen=True)
class BacktestComparison:
    """One saved comparison: the cross-model table and the data version it scored."""

    table: pd.DataFrame
    data_version: str


def version_tags(family: str, train_months: list[str], data_version: str) -> dict[str, str]:
    """Build the three tags that define a model version — its identity in the registry."""
    months = sorted(normalize_month(m) for m in train_months)
    return {
        "model_family": family,
        "train_months": ",".join(months),
        "data_version": data_version,
    }


class MlflowStore:
    """The local MLflow store, created once at an entry point and passed on."""

    def __init__(self, tracking_dir: pathlib.Path | None = None) -> None:
        self.root = (tracking_dir or (ml_dir() / "mlflow")).resolve()

    def activate(self) -> None:
        """Point MLflow's process-wide state at this store."""
        self.root.mkdir(parents=True, exist_ok=True)
        mlflow.set_tracking_uri(f"sqlite:///{self.root / 'mlflow.db'}")

    def set_experiment(self, name: str) -> None:
        """Create the experiment with its files inside the store, then select it."""
        self.activate()
        if mlflow.get_experiment_by_name(name) is None:
            mlflow.create_experiment(name, artifact_location=(self.root / "artifacts").as_uri())
        mlflow.set_experiment(name)

    def _client(self) -> MlflowClient:
        self.activate()
        return MlflowClient()

    def find_version(
        self, family: str, train_months: list[str], data_version: str
    ) -> ModelVersion | None:
        """Return the registered version with the same identity tags, or None."""
        client = self._client()
        wanted = version_tags(family, train_months, data_version)
        versions = client.search_model_versions(f"name = '{REGISTERED_MODEL_NAME}'")
        matches = [v for v in versions if {key: v.tags.get(key) for key in wanted} == wanted]
        return max(matches, key=lambda v: int(v.version), default=None)

    def register_version(
        self,
        model: DemandModel,
        *,
        train_months: list[str],
        data_version: str,
    ) -> ModelVersion:
        """Save a fitted model as a new version of ``demand-model``."""
        self.set_experiment(TRAINING_EXPERIMENT)
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

        client = self._client()
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

    def latest_registered_version(self) -> ModelVersion | None:
        """Return the newest registered version, or None while the registry is empty."""
        versions = self._client().search_model_versions(f"name = '{REGISTERED_MODEL_NAME}'")
        return max(versions, key=lambda v: int(v.version), default=None)

    def champion_version(self) -> ModelVersion | None:
        """Return the version the champion alias points at, or None before the first promotion."""
        client = self._client()
        try:
            return client.get_model_version_by_alias(REGISTERED_MODEL_NAME, CHAMPION_ALIAS)
        except MlflowException as error:
            if error.error_code in ("RESOURCE_DOES_NOT_EXIST", "INVALID_PARAMETER_VALUE"):
                return None
            raise

    def promote_to_champion(self, version: ModelVersion) -> None:
        """Move the champion alias to ``version``; the old champion becomes a challenger."""
        client = self._client()
        previous = self.champion_version()
        # The store returns version numbers as int or str depending on the
        # backend; compare them as strings.
        if previous is not None and str(previous.version) != str(version.version):
            client.set_model_version_tag(
                REGISTERED_MODEL_NAME, previous.version, "role", "challenger"
            )
        client.set_registered_model_alias(REGISTERED_MODEL_NAME, CHAMPION_ALIAS, version.version)
        client.set_model_version_tag(REGISTERED_MODEL_NAME, version.version, "role", "champion")

    def mark_challenger(self, version: ModelVersion) -> None:
        """Tag a version that lost the comparison; it waits for the next one."""
        self._client().set_model_version_tag(
            REGISTERED_MODEL_NAME, version.version, "role", "challenger"
        )

    def load_version_model(self, version: ModelVersion) -> DemandModel:
        """Download a version's files and load the fitted model behind the interface."""
        self.activate()
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

    def resolve_champion(self) -> tuple[DemandModel, ModelVersion]:
        """Return the champion, loaded and ready to predict, with its version."""
        version = self.champion_version()
        if version is None:
            raise LookupError(
                "the registry has no champion yet; run the retraining pipeline "
                "first (python -m gbp.ml.pipeline)"
            )
        return self.load_version_model(version), version

    def log_comparison(self, table: pd.DataFrame, *, params: dict[str, object]) -> None:
        """Write the backtest comparison: its parameters and the table as one CSV."""
        self.set_experiment(BACKTEST_EXPERIMENT)
        with mlflow.start_run(run_name=_COMPARISON_RUN_NAME):
            mlflow.log_params(params)
            with tempfile.TemporaryDirectory() as folder:
                path = pathlib.Path(folder) / _COMPARISON_FILE
                table.to_csv(path, index=False)
                mlflow.log_artifact(str(path))

    def latest_comparison(self) -> BacktestComparison | None:
        """Read the newest saved comparison back, or None if there is none."""
        self.activate()
        experiment = mlflow.get_experiment_by_name(BACKTEST_EXPERIMENT)
        if experiment is None:
            return None
        runs = typing.cast(
            pd.DataFrame,
            mlflow.search_runs(
                [experiment.experiment_id],
                filter_string=f"tags.mlflow.runName = '{_COMPARISON_RUN_NAME}'",
                order_by=["attributes.start_time DESC"],
                max_results=1,
            ),
        )
        if runs.empty:
            return None
        newest = runs.iloc[0]
        with tempfile.TemporaryDirectory() as folder:
            path = mlflow.artifacts.download_artifacts(
                f"runs:/{newest['run_id']}/{_COMPARISON_FILE}", dst_path=folder
            )
            table = pd.read_csv(path)
        return BacktestComparison(
            table=table, data_version=str(newest.get("params.data_version", ""))
        )
