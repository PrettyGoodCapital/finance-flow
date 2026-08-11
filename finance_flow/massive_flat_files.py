from datetime import date
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
            return MassiveDailyBarsFlatFileTransformResult(
                source_path=str(context.source_path), output_path=str(context.output_path), status="exists"
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

                ticker = pc.ascii_upper(pc.utf8_trim_whitespace(batch.column(batch.schema.get_field_index("ticker"))))
                tickers = ticker.to_pylist()
                duplicates = seen_tickers.intersection(tickers)
                if len(tickers) != len(set(tickers)) or duplicates:
                    raise ValueError(f"Duplicate Massive daily bars for {sorted(duplicates or set(tickers))[:1]}.")
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


class MassiveDailyBarsFlatFileModel(CallableModel):
    materializer: ArtifactMaterializeModel
    transform: MassiveDailyBarsFlatFileTransformModel
    local_writer: ArtifactWriteFileModel
    backup_writer: ArtifactWriteFileModel | None = None
    workspace: Path = Path("data/workspace")
    input_key_template: str = "massive/stocks/s3/day-aggs/{year}/{month}/{date}.csv.gz"
    output_key_template: str = "massive/stocks/curated/bars/daily/v1/{year}/{month}/{date}.parquet"
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
        statuses = [local_write.status]
        if backup_write is not None:
            statuses.append(backup_write.status)
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
        )


def _format_key(template: str, value: date) -> str:
    return template.format(date=value.isoformat(), year=f"{value.year:04d}", month=f"{value.month:02d}")
