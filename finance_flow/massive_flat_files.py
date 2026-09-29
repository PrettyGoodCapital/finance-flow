import json
from datetime import UTC, date, datetime
from hashlib import sha256
from importlib.metadata import version
from pathlib import Path
from typing import Any
from uuid import uuid4

from ccflow import CallableModel, ContextBase, ContextType, DateContext, Flow, ResultBase, ResultType
from ccflow_etl import (
    ArtifactMaterializeContext,
    ArtifactMaterializeModel,
    ArtifactMaterializeResult,
    ArtifactWriteFileContext,
    ArtifactWriteFileModel,
    ArtifactWriteFileResult,
)
from pydantic import Field

__all__ = (
    "MassiveDailyBarsFlatFileContext",
    "MassiveDailyBarsFlatFileModel",
    "MassiveDailyBarsFlatFileResult",
    "MassiveDailyBarsFlatFileTransformContext",
    "MassiveDailyBarsFlatFileTransformModel",
    "MassiveDailyBarsFlatFileTransformResult",
)


class MassiveDailyBarsFlatFileTransformContext(ContextBase):
    source_path: Path
    output_path: Path
    session_date: date
    source_key: str | None = None
    overwrite: bool = False
    dry_run: bool = False


class MassiveDailyBarsFlatFileTransformResult(ResultBase):
    source_path: str
    output_path: str
    status: str
    row_count: int | None = None
    quality: dict[str, Any] = Field(default_factory=dict)


class MassiveDailyBarsFlatFileTransformModel(CallableModel):
    batch_size: int = 1 << 20
    compression: str = "zstd"
    schema_version: str = "1"

    @property
    def context_type(self) -> type[ContextType]:
        return MassiveDailyBarsFlatFileTransformContext

    @property
    def result_type(self) -> type[ResultType]:
        return MassiveDailyBarsFlatFileTransformResult

    @Flow.call
    def __call__(self, context: MassiveDailyBarsFlatFileTransformContext) -> MassiveDailyBarsFlatFileTransformResult:
        if context.dry_run:
            return MassiveDailyBarsFlatFileTransformResult(
                source_path=str(context.source_path), output_path=str(context.output_path), status="planned"
            )
        if context.output_path.exists() and not context.overwrite:
            quality = _parquet_quality(context.output_path)
            return MassiveDailyBarsFlatFileTransformResult(
                source_path=str(context.source_path),
                output_path=str(context.output_path),
                status="exists",
                row_count=quality["row_count"],
                quality=quality,
            )

        try:
            import pyarrow as pa
            import pyarrow.compute as pc
            import pyarrow.parquet as pq
            from pyarrow import csv
        except ImportError as exc:
            raise ImportError("Massive flat-file transforms require pyarrow.") from exc

        schema = pa.schema(
            [
                ("ticker", pa.string()),
                ("date", pa.date32()),
                ("open", pa.float64()),
                ("high", pa.float64()),
                ("low", pa.float64()),
                ("close", pa.float64()),
                ("volume", pa.float64()),
                ("vwap", pa.float64()),
                ("transactions", pa.int64()),
            ],
            metadata={
                b"dataset": b"massive-stocks-bars-daily",
                b"provider": b"massive",
                b"schema_name": b"daily_bar",
                b"schema_version": self.schema_version.encode(),
                b"source_key": (context.source_key or "").encode(),
            },
        )
        column_types = {
            "ticker": pa.string(),
            "volume": pa.float64(),
            "open": pa.float64(),
            "close": pa.float64(),
            "high": pa.float64(),
            "low": pa.float64(),
            "window_start": pa.int64(),
            "transactions": pa.int64(),
        }
        reader = csv.open_csv(
            str(context.source_path),
            read_options=csv.ReadOptions(block_size=self.batch_size),
            convert_options=csv.ConvertOptions(column_types=column_types, include_columns=list(column_types)),
        )

        context.output_path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = context.output_path.with_name(f".{context.output_path.name}.{uuid4().hex}.tmp")
        row_count = 0
        seen_tickers: set[str] = set()
        writer = None
        try:
            writer = pq.ParquetWriter(temp_path, schema=schema, compression=self.compression, use_dictionary=["ticker"])
            for batch in reader:
                required = [batch.column(batch.schema.get_field_index(name)) for name in column_types]
                if any(bool(pc.any(pc.is_null(column)).as_py()) for column in required):
                    raise ValueError("Massive daily aggregate contains null required values.")

                ticker = pc.utf8_trim_whitespace(batch.column(batch.schema.get_field_index("ticker")))
                tickers = ticker.to_pylist()
                duplicates = seen_tickers.intersection(tickers)
                batch_tickers: set[str] = set()
                for value in tickers:
                    if value in batch_tickers:
                        duplicates.add(value)
                    batch_tickers.add(value)
                if duplicates:
                    raise ValueError(f"Duplicate Massive daily bars for {sorted(duplicates)[:1]}.")
                seen_tickers.update(tickers)

                volume = batch.column(batch.schema.get_field_index("volume"))
                transactions = batch.column(batch.schema.get_field_index("transactions"))
                if bool(pc.any(pc.less(volume, 0)).as_py()) or bool(pc.any(pc.less(transactions, 0)).as_py()):
                    raise ValueError("Massive daily aggregate volume and transactions must be non-negative.")

                open_ = batch.column(batch.schema.get_field_index("open"))
                high = batch.column(batch.schema.get_field_index("high"))
                low = batch.column(batch.schema.get_field_index("low"))
                close = batch.column(batch.schema.get_field_index("close"))
                invalid_high = pc.or_(pc.less(high, open_), pc.or_(pc.less(high, low), pc.less(high, close)))
                invalid_low = pc.or_(pc.greater(low, open_), pc.or_(pc.greater(low, high), pc.greater(low, close)))
                if bool(pc.any(pc.or_(invalid_high, invalid_low)).as_py()):
                    raise ValueError("Massive daily aggregate violates OHLC bounds.")

                output_batch = pa.RecordBatch.from_arrays(
                    [
                        ticker,
                        pa.array([context.session_date] * batch.num_rows, type=pa.date32()),
                        open_,
                        high,
                        low,
                        close,
                        volume,
                        pa.nulls(batch.num_rows, type=pa.float64()),
                        transactions,
                    ],
                    schema=schema,
                )
                writer.write_batch(output_batch)
                row_count += batch.num_rows
            writer.close()
            writer = None
            temp_path.replace(context.output_path)
        except Exception:
            if writer is not None:
                writer.close()
            temp_path.unlink(missing_ok=True)
            raise

        return MassiveDailyBarsFlatFileTransformResult(
            source_path=str(context.source_path),
            output_path=str(context.output_path),
            status="transformed",
            row_count=row_count,
            quality=_parquet_quality(context.output_path),
        )


class MassiveDailyBarsFlatFileContext(DateContext):
    dry_run: bool = False


class MassiveDailyBarsFlatFileResult(ResultBase):
    date: date
    input_key: str
    output_key: str
    status: str
    materialization: ArtifactMaterializeResult
    transform: MassiveDailyBarsFlatFileTransformResult
    local_write: ArtifactWriteFileResult
    backup_write: ArtifactWriteFileResult | None = None
    sidecar_key: str
    sidecar_local_write: ArtifactWriteFileResult
    sidecar_backup_write: ArtifactWriteFileResult | None = None


class MassiveDailyBarsFlatFileModel(CallableModel):
    materializer: ArtifactMaterializeModel
    transform: MassiveDailyBarsFlatFileTransformModel
    local_writer: ArtifactWriteFileModel
    backup_writer: ArtifactWriteFileModel | None = None
    workspace: Path = Path("data/workspace")
    input_key_template: str = "massive/stocks/s3/day-aggs/{year}/{month}/{date}.csv.gz"
    output_key_template: str = "massive/stocks/curated/bars/daily/v1/{year}/{month}/{date}.parquet"
    sidecar_key_template: str = "massive/stocks/curated/bars/daily/v1/{year}/{month}/{date}.metadata.json"
    overwrite: bool = False
    explain: bool = False

    @property
    def context_type(self) -> type[ContextType]:
        return MassiveDailyBarsFlatFileContext

    @property
    def result_type(self) -> type[ResultType]:
        return MassiveDailyBarsFlatFileResult

    def input_key(self, context: MassiveDailyBarsFlatFileContext) -> str:
        return _format_key(self.input_key_template, context.date)

    def output_key(self, context: MassiveDailyBarsFlatFileContext) -> str:
        return _format_key(self.output_key_template, context.date)

    def source_path(self, context: MassiveDailyBarsFlatFileContext) -> Path:
        return self.workspace / "raw" / self.input_key(context)

    def output_path(self, context: MassiveDailyBarsFlatFileContext) -> Path:
        return self.workspace / "curated" / self.output_key(context)

    def sidecar_key(self, context: MassiveDailyBarsFlatFileContext) -> str:
        return _format_key(self.sidecar_key_template, context.date)

    def sidecar_path(self, context: MassiveDailyBarsFlatFileContext) -> Path:
        return self.workspace / "curated" / self.sidecar_key(context)

    def _sidecar(
        self,
        context: MassiveDailyBarsFlatFileContext,
        output_key: str,
        output_path: Path,
        materialization: ArtifactMaterializeResult,
        transform: MassiveDailyBarsFlatFileTransformResult,
    ) -> dict[str, Any]:
        source = {"key": materialization.key, "uri": materialization.uri, "size": materialization.size}
        source.update(
            {
                name: materialization.metadata[name]
                for name in ("bucket", "object", "etag", "version_id", "last_modified")
                if materialization.metadata.get(name) is not None
            }
        )
        transform_config = json.dumps(self.transform.model_dump(mode="json", exclude={"meta"}), sort_keys=True, separators=(",", ":"))
        return {
            "dataset": "massive-stocks-bars-daily",
            "schema_name": "daily_bar",
            "schema_version": self.transform.schema_version,
            "date": context.date.isoformat(),
            "source": source,
            "transform": {
                "model": f"{type(self.transform).__module__}.{type(self.transform).__qualname__}",
                "config_fingerprint": sha256(transform_config.encode()).hexdigest(),
                "versions": {"finance-flow": version("finance-flow"), "pyarrow": version("pyarrow")},
            },
            "output": {"key": output_key, "size": output_path.stat().st_size, "sha256": _file_sha256(output_path), "row_count": transform.row_count},
            "quality": transform.quality,
            "produced_at": datetime.now(UTC).isoformat(),
        }

    def _write_sidecar(self, path: Path, sidecar: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
        temp_path.write_text(json.dumps(sidecar, indent=2, sort_keys=True) + "\n")
        temp_path.replace(path)

    def _materialize_context(self, context: MassiveDailyBarsFlatFileContext, dry_run: bool | None = None) -> ArtifactMaterializeContext:
        return ArtifactMaterializeContext(
            key=self.input_key(context),
            path=self.source_path(context),
            overwrite=self.overwrite,
            dry_run=context.dry_run or self.explain if dry_run is None else dry_run,
            metadata={"date": context.date.isoformat(), "dataset": "massive-stocks-day-aggs-raw"},
        )

    @Flow.deps
    def __deps__(self, context: MassiveDailyBarsFlatFileContext) -> list[tuple[CallableModel, list[ContextType]]]:
        return [(self.materializer, [self._materialize_context(context)])]

    @Flow.call
    def __call__(self, context: MassiveDailyBarsFlatFileContext) -> MassiveDailyBarsFlatFileResult:
        dry_run = context.dry_run or self.explain
        input_key = self.input_key(context)
        output_key = self.output_key(context)
        source_path = self.source_path(context)
        output_path = self.output_path(context)
        metadata: dict[str, Any] = {
            "date": context.date.isoformat(),
            "provider": "massive",
            "schema_name": "daily_bar",
            "schema_version": "1",
            "source_key": input_key,
        }

        materialization = self.materializer(self._materialize_context(context, dry_run=dry_run))
        transform = self.transform(
            MassiveDailyBarsFlatFileTransformContext(
                source_path=source_path,
                output_path=output_path,
                session_date=context.date,
                source_key=input_key,
                overwrite=self.overwrite,
                dry_run=dry_run,
            )
        )
        local_write = self.local_writer(
            ArtifactWriteFileContext(
                key=output_key,
                path=output_path,
                media_type="application/vnd.apache.parquet",
                dataset="massive-stocks-bars-daily",
                stage="transform",
                overwrite=self.overwrite,
                dry_run=dry_run,
                metadata={**metadata, "row_count": transform.row_count} if transform.row_count is not None else metadata,
            )
        )
        backup_write = None
        if self.backup_writer is not None:
            backup_write = self.backup_writer(
                ArtifactWriteFileContext(
                    key=output_key,
                    path=output_path,
                    media_type="application/vnd.apache.parquet",
                    dataset="massive-stocks-bars-daily",
                    stage="load",
                    overwrite=self.overwrite,
                    dry_run=dry_run,
                    metadata={**metadata, "row_count": transform.row_count} if transform.row_count is not None else metadata,
                )
            )

        sidecar_key = self.sidecar_key(context)
        sidecar_path = self.sidecar_path(context)
        if not dry_run and (self.overwrite or transform.status == "transformed" or not sidecar_path.exists()):
            self._write_sidecar(sidecar_path, self._sidecar(context, output_key, output_path, materialization, transform))
        sidecar_context = {
            "key": sidecar_key,
            "path": sidecar_path,
            "media_type": "application/json",
            "dataset": "massive-stocks-bars-daily",
            "overwrite": self.overwrite,
            "dry_run": dry_run,
            "metadata": {"date": context.date.isoformat(), "output_key": output_key},
        }
        sidecar_local_write = self.local_writer(ArtifactWriteFileContext(**sidecar_context, stage="transform"))
        sidecar_backup_write = None
        if self.backup_writer is not None:
            sidecar_backup_write = self.backup_writer(ArtifactWriteFileContext(**sidecar_context, stage="load"))

        statuses = [local_write.status, sidecar_local_write.status]
        if backup_write is not None:
            statuses.append(backup_write.status)
        if sidecar_backup_write is not None:
            statuses.append(sidecar_backup_write.status)
        status = "planned" if dry_run else next((value for value in statuses if value != "exists"), "exists")
        return MassiveDailyBarsFlatFileResult(
            date=context.date,
            input_key=input_key,
            output_key=output_key,
            status=status,
            materialization=materialization,
            transform=transform,
            local_write=local_write,
            backup_write=backup_write,
            sidecar_key=sidecar_key,
            sidecar_local_write=sidecar_local_write,
            sidecar_backup_write=sidecar_backup_write,
        )


def _parquet_quality(path: Path) -> dict[str, Any]:
    """Recompute daily-bar quality counters from a written partition; one session is small enough to read whole."""
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    table = pq.read_table(path, columns=["ticker", "open", "high", "low", "close", "volume", "transactions"])

    def count(mask: Any) -> int:
        return int(pc.sum(mask).as_py() or 0)

    open_, high, low, close = (table.column(name) for name in ("open", "high", "low", "close"))
    volume, transactions = table.column("volume"), table.column("transactions")
    ticker_count = int(pc.count_distinct(table.column("ticker")).as_py())
    invalid_high = pc.or_(pc.less(high, open_), pc.or_(pc.less(high, low), pc.less(high, close)))
    invalid_low = pc.or_(pc.greater(low, open_), pc.or_(pc.greater(low, high), pc.greater(low, close)))
    return {
        "row_count": table.num_rows,
        "ticker_count": ticker_count,
        "duplicate_ticker_count": table.num_rows - ticker_count,
        "null_required_count": sum(column.null_count for column in table.columns),
        "negative_volume_or_transactions_count": count(pc.or_(pc.less(volume, 0), pc.less(transactions, 0))),
        "ohlc_violation_count": count(pc.or_(invalid_high, invalid_low)),
        "zero_volume_count": count(pc.equal(volume, 0)),
        "zero_transactions_count": count(pc.equal(transactions, 0)),
    }


def _file_sha256(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as file:
        for block in iter(lambda: file.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def _format_key(template: str, value: date) -> str:
    return template.format(date=value.isoformat(), year=f"{value.year:04d}", month=f"{value.month:02d}")
