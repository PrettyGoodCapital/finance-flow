import json
from datetime import date
from gzip import compress
from hashlib import sha256

import pyarrow.parquet as pq
import pytest
from ccflow_etl import ArtifactMaterializeModel, ArtifactWriteFileModel, LocalFileOutput, NoOpArtifactStore
from hydra import compose, initialize_config_dir
from hydra.utils import instantiate

from finance_flow import (
    MassiveDailyBarsFlatFileContext,
    MassiveDailyBarsFlatFileModel,
    MassiveDailyBarsFlatFileTransformContext,
    MassiveDailyBarsFlatFileTransformModel,
)

CSV_PAYLOAD = b"""ticker,volume,open,close,high,low,window_start,transactions
BCPC,58414500.125,184.22,184.95,185.88,183.43,1704240000000000000,521321
BCpC,23000000,370.0,374.5,375.0,369.5,1704240000000000000,310000
"""


def test_massive_daily_bars_flat_file_transform_streams_csv_gzip_to_parquet(tmp_path):
    source_path = tmp_path / "2024-01-03.csv.gz"
    output_path = tmp_path / "2024-01-03.parquet"
    source_path.write_bytes(compress(CSV_PAYLOAD))

    result = MassiveDailyBarsFlatFileTransformModel(batch_size=128)(
        MassiveDailyBarsFlatFileTransformContext(
            source_path=source_path,
            output_path=output_path,
            session_date="2024-01-03",
            source_key="massive/stocks/s3/day-aggs/2024/01/2024-01-03.csv.gz",
        )
    )

    assert result.status == "transformed"
    assert result.row_count == 2
    assert pq.read_table(output_path).to_pylist() == [
        {
            "ticker": "BCPC",
            "date": date(2024, 1, 3),
            "open": 184.22,
            "high": 185.88,
            "low": 183.43,
            "close": 184.95,
            "volume": 58414500.125,
            "vwap": None,
            "transactions": 521321,
        },
        {
            "ticker": "BCpC",
            "date": date(2024, 1, 3),
            "open": 370.0,
            "high": 375.0,
            "low": 369.5,
            "close": 374.5,
            "volume": 23000000,
            "vwap": None,
            "transactions": 310000,
        },
    ]
    assert pq.read_schema(output_path).metadata[b"source_key"] == b"massive/stocks/s3/day-aggs/2024/01/2024-01-03.csv.gz"


def test_massive_daily_bars_flat_file_transform_rejects_invalid_ohlc_atomically(tmp_path):
    source_path = tmp_path / "invalid.csv.gz"
    output_path = tmp_path / "daily.parquet"
    source_path.write_bytes(compress(b"ticker,volume,open,close,high,low,window_start,transactions\nAAPL,100,10,11,9,8,1704240000000000000,5\n"))

    with pytest.raises(ValueError, match="OHLC bounds"):
        MassiveDailyBarsFlatFileTransformModel()(
            MassiveDailyBarsFlatFileTransformContext(source_path=source_path, output_path=output_path, session_date="2024-01-03")
        )

    assert not output_path.exists()
    assert not list(tmp_path.glob(".*.tmp"))


def test_massive_daily_bars_flat_file_pipeline_materializes_writes_local_and_backs_up(tmp_path):
    input_store = LocalFileOutput(path=tmp_path / "remote-raw")
    local_store = LocalFileOutput(path=tmp_path / "local-datasets")
    backup_store = LocalFileOutput(path=tmp_path / "remote-curated")
    input_key = "massive/stocks/s3/day-aggs/2024/01/2024-01-03.csv.gz"
    output_key = "massive/stocks/curated/bars/daily/v1/2024/01/2024-01-03.parquet"
    input_store.write(input_key, compress(CSV_PAYLOAD))
    model = MassiveDailyBarsFlatFileModel(
        materializer=ArtifactMaterializeModel(store=input_store),
        transform=MassiveDailyBarsFlatFileTransformModel(batch_size=128),
        local_writer=ArtifactWriteFileModel(store=local_store),
        backup_writer=ArtifactWriteFileModel(store=backup_store),
        workspace=tmp_path / "workspace",
    )

    result = model(MassiveDailyBarsFlatFileContext(date="2024-01-03"))

    assert result.status == "written"
    assert result.materialization.status == "materialized"
    assert result.transform.row_count == 2
    assert result.local_write.status == "written"
    assert result.backup_write.status == "written"
    assert pq.read_table(local_store.file_path(output_key)).num_rows == 2
    assert pq.read_table(backup_store.file_path(output_key)).num_rows == 2


def test_massive_daily_bars_flat_file_pipeline_explain_is_io_free(tmp_path):
    store = NoOpArtifactStore()
    model = MassiveDailyBarsFlatFileModel(
        materializer=ArtifactMaterializeModel(store=store),
        transform=MassiveDailyBarsFlatFileTransformModel(),
        local_writer=ArtifactWriteFileModel(store=store),
        backup_writer=ArtifactWriteFileModel(store=store),
        workspace=tmp_path,
        explain=True,
    )

    result = model(MassiveDailyBarsFlatFileContext(date="2024-01-03"))

    assert result.status == "planned"
    assert result.materialization.status == "planned"
    assert result.transform.status == "planned"
    assert result.local_write.status == "planned"
    assert result.backup_write.status == "planned"
    assert result.sidecar_local_write.status == "planned"
    assert result.sidecar_backup_write.status == "planned"
    assert not list(tmp_path.rglob("*"))


def test_massive_daily_bars_flat_file_task_config_resolves_current_task_shape(tmp_path):
    (tmp_path / "runner.yaml").write_text(
        """
defaults:
    - _self_
    - output: /outputs/noop
    - task: /tasks/massive/stocks/daily-bars-flat-file

callable: /task

hydra:
    searchpath:
        - pkg://ccflow_etl.config
        - pkg://finance_flow.config
""".lstrip()
    )

    with initialize_config_dir(config_dir=str(tmp_path), version_base=None):
        cfg = compose(config_name="runner")

    task = instantiate(cfg.task)

    assert isinstance(task, MassiveDailyBarsFlatFileModel)
    assert isinstance(task.materializer, ArtifactMaterializeModel)
    assert isinstance(task.local_writer, ArtifactWriteFileModel)
    assert task.explain is True
    assert cfg.callable == "/task"


class IdentityFileOutput(LocalFileOutput):
    def read_file(self, key, path):
        return {**super().read_file(key, path), "etag": '"raw-etag"', "version_id": "v-3", "last_modified": "2024-01-04T05:45:00+00:00"}


def _pipeline(tmp_path, **kwargs):
    input_store = IdentityFileOutput(path=tmp_path / "remote-raw")
    input_store.write("massive/stocks/s3/day-aggs/2024/01/2024-01-03.csv.gz", compress(CSV_PAYLOAD))
    local_store = LocalFileOutput(path=tmp_path / "local-datasets")
    backup_store = LocalFileOutput(path=tmp_path / "remote-curated")
    model = MassiveDailyBarsFlatFileModel(
        materializer=ArtifactMaterializeModel(store=input_store),
        transform=MassiveDailyBarsFlatFileTransformModel(batch_size=128),
        local_writer=ArtifactWriteFileModel(store=local_store),
        backup_writer=ArtifactWriteFileModel(store=backup_store),
        workspace=tmp_path / "workspace",
        **kwargs,
    )
    return model, local_store, backup_store


def test_massive_daily_bars_flat_file_pipeline_writes_lineage_and_quality_sidecar(tmp_path):
    model, local_store, backup_store = _pipeline(tmp_path)
    sidecar_key = "massive/stocks/curated/bars/daily/v1/2024/01/2024-01-03.metadata.json"
    output_key = "massive/stocks/curated/bars/daily/v1/2024/01/2024-01-03.parquet"

    result = model(MassiveDailyBarsFlatFileContext(date="2024-01-03"))
    sidecar = json.loads(local_store.file_path(sidecar_key).read_text())
    published = local_store.file_path(output_key)

    assert result.sidecar_key == sidecar_key
    assert result.sidecar_local_write.status == "written"
    assert result.sidecar_backup_write.status == "written"
    assert backup_store.file_path(sidecar_key).read_text() == local_store.file_path(sidecar_key).read_text()
    assert sidecar["source"] == {
        "key": "massive/stocks/s3/day-aggs/2024/01/2024-01-03.csv.gz",
        "uri": result.materialization.uri,
        "size": result.materialization.size,
        "etag": '"raw-etag"',
        "version_id": "v-3",
        "last_modified": "2024-01-04T05:45:00+00:00",
    }
    assert sidecar["output"] == {
        "key": output_key,
        "size": published.stat().st_size,
        "sha256": sha256(published.read_bytes()).hexdigest(),
        "row_count": 2,
    }
    assert sidecar["quality"] == {
        "row_count": 2,
        "ticker_count": 2,
        "duplicate_ticker_count": 0,
        "null_required_count": 0,
        "negative_volume_or_transactions_count": 0,
        "ohlc_violation_count": 0,
        "zero_volume_count": 0,
        "zero_transactions_count": 0,
    }
    assert sidecar["transform"]["provenance"] == "recorded"
    assert sidecar["transform"]["model"] == "finance_flow.massive_flat_files.MassiveDailyBarsFlatFileTransformModel"
    assert len(sidecar["transform"]["config_fingerprint"]) == 64
    assert set(sidecar["transform"]["versions"]) == {"finance-flow", "pyarrow"}
    assert sidecar["schema_version"] == "1"


def test_massive_daily_bars_flat_file_pipeline_adds_missing_sidecar_to_existing_partition(tmp_path):
    model, local_store, backup_store = _pipeline(tmp_path)
    context = MassiveDailyBarsFlatFileContext(date="2024-01-03")
    sidecar_key = model.sidecar_key(context)
    model(context)
    for path in (model.sidecar_path(context), local_store.file_path(sidecar_key), backup_store.file_path(sidecar_key)):
        path.unlink()

    result = model(context)

    assert result.transform.status == "exists"
    assert result.local_write.status == "exists"
    assert result.backup_write.status == "exists"
    assert result.sidecar_local_write.status == "written"
    assert result.sidecar_backup_write.status == "written"
    assert result.status == "written"
    sidecar = json.loads(backup_store.file_path(sidecar_key).read_text())
    assert sidecar["quality"]["row_count"] == 2
    assert sidecar["transform"] == {"provenance": "backfilled", "model": None, "config_fingerprint": None, "versions": None}


def test_massive_daily_bars_flat_file_pipeline_skips_sidecar_when_rebuilt_file_was_not_published(tmp_path):
    model, local_store, backup_store = _pipeline(tmp_path)
    context = MassiveDailyBarsFlatFileContext(date="2024-01-03")
    sidecar_key = model.sidecar_key(context)
    model(context)
    for path in (model.output_path(context), model.sidecar_path(context), local_store.file_path(sidecar_key), backup_store.file_path(sidecar_key)):
        path.unlink()

    result = model(context)

    assert result.transform.status == "transformed"
    assert result.local_write.status == "exists"
    assert result.sidecar_local_write is None
    assert result.sidecar_backup_write is None
    assert not local_store.file_path(sidecar_key).exists()
    assert not backup_store.file_path(sidecar_key).exists()


def test_massive_daily_bars_flat_file_pipeline_regenerates_sidecars_without_touching_partitions(tmp_path):
    model, local_store, backup_store = _pipeline(tmp_path)
    context = MassiveDailyBarsFlatFileContext(date="2024-01-03")
    model(context)
    partition = local_store.file_path(model.output_key(context))
    partition_bytes = partition.read_bytes()

    result = model.model_copy(update={"overwrite_sidecar": True})(context)

    assert result.local_write.status == "exists"
    assert result.sidecar_local_write.status == "written"
    assert result.sidecar_backup_write.status == "written"
    assert partition.read_bytes() == partition_bytes
    assert json.loads(backup_store.file_path(model.sidecar_key(context)).read_text())["transform"]["provenance"] == "backfilled"


def test_massive_daily_bars_flat_file_transform_reports_zero_activity_counts(tmp_path):
    source_path = tmp_path / "zero.csv.gz"
    source_path.write_bytes(
        compress(
            b"ticker,volume,open,close,high,low,window_start,transactions\nAAA,0,10,10,10,10,1704240000000000000,0\nBBB,5,10,11,12,9,1704240000000000000,2\n"
        )
    )

    result = MassiveDailyBarsFlatFileTransformModel()(
        MassiveDailyBarsFlatFileTransformContext(source_path=source_path, output_path=tmp_path / "zero.parquet", session_date="2024-01-03")
    )

    assert result.quality["zero_volume_count"] == 1
    assert result.quality["zero_transactions_count"] == 1
    assert result.quality["ohlc_violation_count"] == 0
