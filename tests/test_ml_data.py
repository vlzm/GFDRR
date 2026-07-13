"""Tests for the forecasting data helpers (``gbp/ml/data.py``).

The daily weather download and its year-file cache — the network itself is
stubbed out. The trip downloader's tests live in ``tests/test_download.py``.
"""

import pandas as pd
import pytest

from gbp.ml import data


@pytest.fixture
def raw(tmp_path):
    """Make a data/raw-like folder for the cached weather year files."""
    folder = tmp_path / "raw"
    folder.mkdir()
    return folder


def write_csv(folder, name, lines):
    path = folder / name
    path.write_text("\n".join(lines) + "\n")
    return path


# ---------------------------------------------------------------------------
# Daily weather (network stubbed out)
# ---------------------------------------------------------------------------
WEATHER_HEADER = '"STATION","DATE","PRCP","TMAX","TMIN"'


def weather_line(date, prcp, tmax, tmin):
    return f'"USW00094728","{date}","{prcp}","{tmax}","{tmin}"'


def test_load_weather_daily_reads_the_cached_year_file(raw, monkeypatch):
    write_csv(
        raw,
        "weather-central-park-2025.csv",
        [
            WEATHER_HEADER,
            weather_line("2025-02-01", "0.0", "8.9", "-6.0"),
            weather_line("2025-02-02", "2.8", "-0.5", "-8.2"),
            weather_line("2025-02-03", "0.0", "9.4", "-0.5"),
        ],
    )

    def no_network(*args, **kwargs):
        raise AssertionError("the network must not be touched for a covered range")

    monkeypatch.setattr(data, "_download_weather_year", no_network)
    weather = data.load_weather_daily(pd.Timestamp("2025-02-01"), pd.Timestamp("2025-02-02"), raw)

    assert weather["date"].tolist() == [pd.Timestamp("2025-02-01"), pd.Timestamp("2025-02-02")]
    assert weather["temperature_max_c"].tolist() == [8.9, -0.5]
    assert weather["temperature_min_c"].tolist() == [-6.0, -8.2]
    assert weather["precipitation_mm"].tolist() == [0.0, 2.8]


def test_load_weather_daily_refetches_a_year_file_that_is_behind(raw, monkeypatch):
    write_csv(
        raw,
        "weather-central-park-2025.csv",
        [WEATHER_HEADER, weather_line("2025-02-01", "0.0", "8.9", "-6.0")],
    )

    def fake_download(year, dest):
        write_csv(
            raw,
            dest.name,
            [
                WEATHER_HEADER,
                weather_line("2025-02-01", "0.0", "8.9", "-6.0"),
                weather_line("2025-02-02", "2.8", "-0.5", "-8.2"),
            ],
        )

    monkeypatch.setattr(data, "_download_weather_year", fake_download)
    weather = data.load_weather_daily(pd.Timestamp("2025-02-01"), pd.Timestamp("2025-02-02"), raw)
    assert len(weather) == 2
