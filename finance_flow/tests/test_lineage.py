from datetime import date

from ccflow_etl import DatasetDefinition, LocalFileOutput

from finance_flow.lineage import source_identity, transform_provenance
from finance_flow.securities import SecurityListingsModel


class HeadStore(LocalFileOutput):
    def head(self, key):
        return {"etag": '"abc"', "version_id": None, "size": 12, "last_modified": "2025-01-06T00:00:00+00:00", "bucket": "b"}


def test_source_identity_records_what_the_store_reports(tmp_path):
    dataset = DatasetDefinition(name="raw", key_template="raw/{date}.json", store=HeadStore(path=tmp_path), format="json")

    identity = source_identity(dataset, date(2025, 1, 6))

    assert identity["uri"].endswith("raw/2025-01-06.json")
    assert {name: identity[name] for name in ("etag", "size", "last_modified")} == {
        "etag": '"abc"',
        "size": 12,
        "last_modified": "2025-01-06T00:00:00+00:00",
    }
    assert "version_id" not in identity and "bucket" not in identity


def test_transform_fingerprint_ignores_runtime_flags_and_datasets(tmp_path):
    store = LocalFileOutput(path=tmp_path)
    dataset = DatasetDefinition(name="d", key_template="d/{date}.parquet", store=store)
    model = SecurityListingsModel(snapshot=dataset.model_copy(update={"format": "json"}), output=dataset)

    base = transform_provenance(model)

    assert transform_provenance(model.model_copy(update={"overwrite_output": True}))["config_fingerprint"] == base["config_fingerprint"]
    assert transform_provenance(model.model_copy(update={"min_name_overlap": 0.6}))["config_fingerprint"] != base["config_fingerprint"]
    assert "snapshot" not in base["settings"] and "output" not in base["settings"]
    assert set(base["versions"]) == {"finance-flow", "polars"}
