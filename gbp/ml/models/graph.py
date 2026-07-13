"""GraphSage on the station graph — the research model (plan, phase 4).

The question this model answers: does spatial structure add accuracy over
boosting? A negative answer is a valid result.

Nodes are stations. Edges come from OD flows: :func:`station_graph_edges`
counts the trips between every pair of stations in the raw trip files of the
given months. The graph is part of what ``fit`` learns: when no ``edges_df``
was passed to the constructor, ``fit`` counts the edges from the raw trip
files of its last training month. The counts become edge weights, made symmetric (a trip
connects both of its endpoints) and cut to the ``max_neighbors`` strongest
neighbors per station; the aggregation step divides by each station's total
weight, so a neighbor's influence is its share of the station's traffic.

The model is written in plain torch, without a graph library: with a mean
aggregator, one GraphSage layer is ``relu(W_self·x + W_neigh·(A·x))`` where
``A`` is the weighted, row-normalized adjacency — a single sparse matrix
multiplication. Two such layers let information travel two hops, then a
linear head reads out the log of the expected departure count (Poisson
loss, so predictions are positive).

One training sample is one ``(period, commodity)`` pair: the node feature
matrix holds every station's feature values at that hour, and the graph
carries values between stations. This works because the training table and
the forecast input are full grids — every facility × commodity × period row
exists. Features are z-scored with the training mean and deviation; a NaN
becomes 0 after z-scoring (the mean), and each nullable column gets a 0/1
missing flag so the model can tell a real value from an imputed one. The
bike type enters as a one-hot column. Rows are weighted by
``1 - stockout_share``, like the LightGBM model.

Training reads only the last ``train_window_months`` months of the training
table (default 3): the history features already carry the longer memory,
and the per-hour samples repeat weekly patterns many times over, so more
months add cost much faster than signal. The window is a model parameter
and is logged with every backtest run.
"""

from __future__ import annotations

import json
import pathlib
import warnings
from typing import ClassVar, Self

# On macOS, torch and lightgbm each bring their own OpenMP runtime, and one
# process cannot run both: whichever starts its parallel regions second
# crashes or deadlocks. Two steps keep them apart. Importing lightgbm first
# makes its runtime the primary one, and torch.set_num_threads(1) below
# keeps torch from opening OpenMP regions at all — LightGBM keeps its
# threads, the graph model runs single-threaded.
import lightgbm  # noqa: F401  # isort: skip
import numpy as np
import pandas as pd
import torch

from gbp.loaders.download import load_trips_any_schema, month_csvs, normalize_month
from gbp.ml.features import FEATURE_COLUMNS, HISTORY_FEATURES, WEATHER_FEATURES
from gbp.ml.models.base import DemandModel, fractional_demand

torch.set_num_threads(1)

#: The feature columns that can be NaN and therefore get a missing flag.
_NULLABLE = WEATHER_FEATURES + HISTORY_FEATURES


def station_graph_edges(months: list[str], raw: pathlib.Path | None = None) -> pd.DataFrame:
    """Count trips between station pairs in the raw files of ``months``.

    Returns one row per ordered pair: ``source_id``, ``target_id``,
    ``trips``. Trips that start and end at the same station are dropped —
    a station is not its own neighbor.
    """
    counts: list[pd.DataFrame] = []
    for month in months:
        csvs = month_csvs(normalize_month(month), raw)
        if not csvs:
            raise FileNotFoundError(
                f"no raw CSVs for {month}; download them first (python -m gbp.loaders.download)"
            )
        for path in csvs:
            trips = load_trips_any_schema(str(path))
            counts.append(
                trips.groupby(["start_station_id", "end_station_id"])
                .size()
                .rename("trips")
                .reset_index()
            )
    edges = (
        pd.concat(counts, ignore_index=True)
        .groupby(["start_station_id", "end_station_id"], as_index=False)["trips"]
        .sum()
        .rename(columns={"start_station_id": "source_id", "end_station_id": "target_id"})
    )
    return edges[edges["source_id"] != edges["target_id"]].reset_index(drop=True)


class _SageNet(torch.nn.Module):
    """Two GraphSage layers and a linear head that outputs a log-rate."""

    def __init__(self, in_dim: int, hidden_size: int) -> None:
        super().__init__()
        self.self1 = torch.nn.Linear(in_dim, hidden_size)
        self.neigh1 = torch.nn.Linear(in_dim, hidden_size, bias=False)
        self.self2 = torch.nn.Linear(hidden_size, hidden_size)
        self.neigh2 = torch.nn.Linear(hidden_size, hidden_size, bias=False)
        self.head = torch.nn.Linear(hidden_size, 1)

    def forward(self, x: torch.Tensor, adjacency: torch.Tensor) -> torch.Tensor:
        """Map node features ``(batch, nodes, in_dim)`` to log-rates ``(batch, nodes)``."""
        h = torch.relu(self.self1(x) + self.neigh1(_aggregate(adjacency, x)))
        h = torch.relu(self.self2(h) + self.neigh2(_aggregate(adjacency, h)))
        out: torch.Tensor = self.head(h)
        return out.squeeze(-1)


def _aggregate(adjacency: torch.Tensor, x: torch.Tensor) -> torch.Tensor:
    """Weighted neighbor mean: sparse ``(nodes, nodes)`` times batched features."""
    batch, nodes, dim = x.shape
    flat = x.permute(1, 0, 2).reshape(nodes, batch * dim)
    mixed: torch.Tensor = torch.sparse.mm(adjacency, flat)
    return mixed.reshape(nodes, batch, dim).permute(1, 0, 2)


class _GridBlock:
    """One full grid (periods × facilities × commodities) as node tensors."""

    def __init__(
        self,
        table: pd.DataFrame,
        facilities: list[str],
        features: np.ndarray,
        target: np.ndarray | None,
        weight: np.ndarray | None,
    ) -> None:
        self.table = table
        self.facilities = facilities
        self.features = features
        self.target = target
        self.weight = weight


class GraphSageModel(DemandModel):
    """GraphSage over the station graph, Poisson loss, plain torch."""

    name: ClassVar[str] = "graphsage"

    def __init__(
        self,
        edges_df: pd.DataFrame | None = None,
        hidden_size: int = 64,
        epochs: int = 3,
        batch_size: int = 64,
        learning_rate: float = 1e-3,
        train_window_months: int = 3,
        max_neighbors: int = 32,
        seed: int = 0,
        raw: pathlib.Path | None = None,
    ) -> None:
        """Store the training settings and, when given, the graph edges.

        Without ``edges_df``, ``fit`` counts the edges from the raw trip
        files of its last training month; ``raw`` overrides the folder those
        files are read from.
        """
        self.hidden_size = hidden_size
        self.epochs = epochs
        self.batch_size = batch_size
        self.learning_rate = learning_rate
        self.train_window_months = train_window_months
        self.max_neighbors = max_neighbors
        self.seed = seed
        self.raw = raw
        self._edges: pd.DataFrame | None = (
            None if edges_df is None else self._symmetric_capped(edges_df)
        )
        self._net: _SageNet | None = None
        self._mu: np.ndarray | None = None
        self._sd: np.ndarray | None = None
        self._commodities: list[str] = []

    def params(self) -> dict[str, object]:
        """Return the graph and training settings, for experiment logs."""
        return {
            "hidden_size": self.hidden_size,
            "epochs": self.epochs,
            "batch_size": self.batch_size,
            "learning_rate": self.learning_rate,
            "train_window_months": self.train_window_months,
            "max_neighbors": self.max_neighbors,
            "seed": self.seed,
            "n_edges": 0 if self._edges is None else len(self._edges),
        }

    def _symmetric_capped(self, edges_df: pd.DataFrame) -> pd.DataFrame:
        """Make the edge weights symmetric, keep the strongest neighbors."""
        swapped = edges_df.rename(columns={"source_id": "target_id", "target_id": "source_id"})
        both = (
            pd.concat([edges_df, swapped], ignore_index=True)
            .groupby(["source_id", "target_id"], as_index=False)["trips"]
            .sum()
        )
        both = both.sort_values("trips", ascending=False, kind="stable")
        both["source_id"] = both["source_id"].astype(str)
        both["target_id"] = both["target_id"].astype(str)
        return both.groupby("source_id").head(self.max_neighbors).reset_index(drop=True)

    def _adjacency(self, facilities: list[str]) -> torch.Tensor:
        """Row-normalized sparse adjacency restricted to ``facilities``."""
        if self._edges is None:
            raise ValueError("graph edges missing — fit the model first")
        index = {facility: i for i, facility in enumerate(facilities)}
        edges = self._edges[
            self._edges["source_id"].isin(index) & self._edges["target_id"].isin(index)
        ]
        n = len(facilities)
        rows = torch.tensor([index[s] for s in edges["source_id"]], dtype=torch.int64)
        cols = torch.tensor([index[t] for t in edges["target_id"]], dtype=torch.int64)
        values = torch.tensor(edges["trips"].to_numpy(), dtype=torch.float32)
        adjacency = torch.sparse_coo_tensor(torch.stack([rows, cols]), values, (n, n)).coalesce()
        row_sum = torch.sparse.sum(adjacency, dim=1).to_dense()
        indices = adjacency.indices()
        normalized = adjacency.values() / row_sum[indices[0]]
        return torch.sparse_coo_tensor(indices, normalized, (n, n)).coalesce()

    def _node_features(self, table: pd.DataFrame) -> np.ndarray:
        """Z-score the feature columns, add missing flags and the bike-type one-hot."""
        if self._mu is None or self._sd is None:
            raise ValueError("feature statistics missing — fit the model first")
        values = table[FEATURE_COLUMNS].to_numpy(dtype="float32", na_value=np.nan)
        scored = (values - self._mu) / self._sd
        flags = np.isnan(table[_NULLABLE].to_numpy(dtype="float32", na_value=np.nan)).astype(
            "float32"
        )
        commodity = table["commodity_category"].astype(str).to_numpy()
        onehot = np.stack([(commodity == c).astype("float32") for c in self._commodities], axis=1)
        return np.concatenate([np.nan_to_num(scored, nan=0.0), flags, onehot], axis=1)

    def _grid_block(self, table: pd.DataFrame, with_target: bool) -> _GridBlock:
        """Sort one full grid into ``(samples, facilities)`` node tensors."""
        table = table.sort_values(
            ["start_timestamp", "commodity_category", "facility_id"], kind="stable"
        ).reset_index(drop=True)
        facilities = sorted(str(f) for f in table["facility_id"].unique())
        samples = table["start_timestamp"].nunique() * table["commodity_category"].nunique()
        if samples * len(facilities) != len(table):
            raise ValueError(
                "the table is not a full grid: every (period, commodity) must "
                "hold one row per facility"
            )
        n_facilities = len(facilities)
        features = self._node_features(table).reshape(samples, n_facilities, -1)
        target = weight = None
        if with_target:
            target = table["quantity"].to_numpy(dtype="float32").reshape(samples, n_facilities)
            weight = np.ones_like(target)
            if "stockout_share" in table.columns:
                share = table["stockout_share"].fillna(0.0).to_numpy(dtype="float32")
                weight = (1.0 - share).clip(min=0.0).reshape(samples, n_facilities)
        return _GridBlock(table, facilities, features, target, weight)

    def fit(self, training_table: pd.DataFrame) -> None:
        """Train on the last ``train_window_months`` months, one month per graph.

        When the constructor got no ``edges_df``, the graph edges are
        counted here, from the raw trip files of the last training month.
        """
        torch.manual_seed(self.seed)
        months = training_table["start_timestamp"].dt.to_period("M")
        keep = sorted(months.unique())[-self.train_window_months :]
        window = training_table[months.isin(keep)]

        if self._edges is None:
            last_month = keep[-1].strftime("%Y%m")
            self._edges = self._symmetric_capped(station_graph_edges([last_month], self.raw))

        values = window[FEATURE_COLUMNS].to_numpy(dtype="float32", na_value=np.nan)
        # A column with no observed value at all (for example weather that
        # was never joined) has no mean or deviation; 0 and 1 keep its
        # z-scores at 0 and its missing flag carries the information.
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", RuntimeWarning)
            self._mu = np.nan_to_num(np.nanmean(values, axis=0), nan=0.0).astype("float32")
            self._sd = np.nan_to_num(np.nanstd(values, axis=0), nan=1.0).astype("float32")
        self._sd[self._sd < 1e-6] = 1.0
        self._commodities = sorted(str(c) for c in window["commodity_category"].unique())

        blocks = [
            self._grid_block(month_table, with_target=True)
            for _, month_table in window.groupby(
                window["start_timestamp"].dt.to_period("M"), observed=True
            )
        ]
        in_dim = blocks[0].features.shape[-1]
        net = _SageNet(in_dim, self.hidden_size)
        optimizer = torch.optim.Adam(net.parameters(), lr=self.learning_rate)
        loss_fn = torch.nn.PoissonNLLLoss(log_input=True, full=False, reduction="none")

        net.train()
        for _ in range(self.epochs):
            for block in blocks:
                adjacency = self._adjacency(block.facilities)
                assert block.target is not None and block.weight is not None
                order = torch.randperm(block.features.shape[0])
                for start in range(0, len(order), self.batch_size):
                    picked = order[start : start + self.batch_size]
                    x = torch.from_numpy(block.features[picked.numpy()])
                    y = torch.from_numpy(block.target[picked.numpy()])
                    w = torch.from_numpy(block.weight[picked.numpy()])
                    optimizer.zero_grad()
                    log_rate = net(x, adjacency)
                    loss = (loss_fn(log_rate, y) * w).mean()
                    loss.backward()
                    optimizer.step()
        net.eval()
        self._net = net

    def predict(self, feature_table: pd.DataFrame) -> pd.DataFrame:
        """Predict the fractional demand for the rows of a forecast input."""
        if self._net is None:
            raise ValueError("fit the model before predicting")
        block = self._grid_block(feature_table, with_target=False)
        adjacency = self._adjacency(block.facilities)
        rates: list[np.ndarray] = []
        with torch.no_grad():
            for start in range(0, block.features.shape[0], self.batch_size):
                x = torch.from_numpy(block.features[start : start + self.batch_size])
                rates.append(torch.exp(self._net(x, adjacency)).numpy())
        quantity = np.concatenate(rates, axis=0).reshape(-1)
        return fractional_demand(block.table, quantity)

    def save(self, folder: pathlib.Path) -> None:
        """Write the network weights, the scaling, and the edges into ``folder``."""
        if self._net is None or self._mu is None or self._sd is None or self._edges is None:
            raise ValueError("fit the model before saving")
        folder.mkdir(parents=True, exist_ok=True)
        torch.save(self._net.state_dict(), folder / "net.pt")
        self._edges.to_parquet(folder / "edges.parquet", index=False)
        (folder / "model.json").write_text(
            json.dumps(
                {
                    "name": self.name,
                    "hidden_size": self.hidden_size,
                    "epochs": self.epochs,
                    "batch_size": self.batch_size,
                    "learning_rate": self.learning_rate,
                    "train_window_months": self.train_window_months,
                    "max_neighbors": self.max_neighbors,
                    "seed": self.seed,
                    "mu": self._mu.tolist(),
                    "sd": self._sd.tolist(),
                    "commodities": self._commodities,
                    "in_dim": int(len(FEATURE_COLUMNS) + len(_NULLABLE) + len(self._commodities)),
                }
            )
        )

    @classmethod
    def load(cls, folder: pathlib.Path) -> Self:
        """Read a model saved by :meth:`save`."""
        saved = json.loads((folder / "model.json").read_text())
        model = cls(
            pd.read_parquet(folder / "edges.parquet"),
            hidden_size=saved["hidden_size"],
            epochs=saved["epochs"],
            batch_size=saved["batch_size"],
            learning_rate=saved["learning_rate"],
            train_window_months=saved["train_window_months"],
            max_neighbors=saved["max_neighbors"],
            seed=saved["seed"],
        )
        model._mu = np.asarray(saved["mu"], dtype="float32")
        model._sd = np.asarray(saved["sd"], dtype="float32")
        model._commodities = saved["commodities"]
        net = _SageNet(saved["in_dim"], saved["hidden_size"])
        net.load_state_dict(torch.load(folder / "net.pt", weights_only=True))
        net.eval()
        model._net = net
        return model
