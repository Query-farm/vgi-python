"""Unit tests for ResolvedSecrets type- and scope-aware selection."""

from vgi.table_function import ResolvedSecrets


def _secrets() -> ResolvedSecrets:
    # Values are plain strings here; ResolvedSecrets also accepts pyarrow Scalars
    # (it calls .as_py() when present).
    return ResolvedSecrets(
        {
            "my_s3": {"type": "s3", "key_id": "AAA", "scope": "s3://bucket-a"},
            "my_s3_b": {
                "type": "s3",
                "key_id": "BBB",
                "scope": "s3://bucket-b\ns3://bucket-b2",
            },
            "my_gcs": {"type": "gcs", "key_id": "G"},
        }
    )


def test_type_aware() -> None:
    """Type-aware accessors find/identify secrets by type."""
    s = _secrets()
    assert s.secret_type("my_s3") == "s3"
    assert s.secret_type("my_gcs") == "gcs"
    assert len(s.of_type("s3")) == 2
    assert len(s.of_type("gcs")) == 1
    assert s.of_type("azure") == []


def test_for_scope_of_type_per_bucket() -> None:
    """Per-bucket scope selection picks the right s3 secret."""
    s = _secrets()
    assert s.for_scope_of_type("s3://bucket-a/x.dat", "s3")["key_id"] == "AAA"
    assert s.for_scope_of_type("s3://bucket-b2/y.dat", "s3")["key_id"] == "BBB"
    assert s.field_for("s3://bucket-a/x.dat", "key_id") == "AAA"


def test_longest_prefix_and_fallback() -> None:
    """Longest scope prefix wins; unscoped is the fallback."""
    s = ResolvedSecrets(
        {
            "broad": {"type": "s3", "key_id": "broad", "scope": "s3://bucket"},
            "narrow": {"type": "s3", "key_id": "narrow", "scope": "s3://bucket/data"},
        }
    )
    assert s.for_scope("s3://bucket/data/x.dat")["key_id"] == "narrow"
    assert s.for_scope("s3://bucket/other/x.dat")["key_id"] == "broad"

    unscoped = ResolvedSecrets({"only": {"type": "s3", "key_id": "only"}})
    assert unscoped.for_scope("s3://any/x")["key_id"] == "only"

    assert s.for_scope("s3://nope/x") is None


def test_dict_access_still_works() -> None:
    """ResolvedSecrets keeps plain dict access."""
    s = _secrets()
    assert s["my_s3"]["key_id"] == "AAA"
    assert s.get("missing") is None
