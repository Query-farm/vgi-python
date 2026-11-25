"""Unit tests for ResolvedSecrets type- and scope-aware selection."""

from typing import Any

from vgi.table_function import ResolvedSecrets


def _key_id(secret: dict[str, Any] | None) -> Any:
    # The scope selectors return None when nothing matches; every call below
    # expects a hit, so unwrap here rather than indexing an Optional inline.
    assert secret is not None
    return secret["key_id"]


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
    assert _key_id(s.for_scope_of_type("s3://bucket-a/x.dat", "s3")) == "AAA"
    assert _key_id(s.for_scope_of_type("s3://bucket-b2/y.dat", "s3")) == "BBB"
    assert s.field_for("s3://bucket-a/x.dat", "key_id") == "AAA"


def test_longest_prefix_and_fallback() -> None:
    """Longest scope prefix wins; unscoped is the fallback."""
    s = ResolvedSecrets(
        {
            "broad": {"type": "s3", "key_id": "broad", "scope": "s3://bucket"},
            "narrow": {"type": "s3", "key_id": "narrow", "scope": "s3://bucket/data"},
        }
    )
    assert _key_id(s.for_scope("s3://bucket/data/x.dat")) == "narrow"
    assert _key_id(s.for_scope("s3://bucket/other/x.dat")) == "broad"

    unscoped = ResolvedSecrets({"only": {"type": "s3", "key_id": "only"}})
    assert _key_id(unscoped.for_scope("s3://any/x")) == "only"

    assert s.for_scope("s3://nope/x") is None


def test_dict_access_still_works() -> None:
    """ResolvedSecrets keeps plain dict access."""
    s = _secrets()
    assert s["my_s3"]["key_id"] == "AAA"
    assert s.get("missing") is None


# Cases follow DuckDB's BaseSecret::MatchScore (Haybarn 2.0.1, duckdb#28684):
# an authority-only URI scope has an implicit trailing "/".
_SCOPE_CASES = [
    # authority-only scopes: exact host, or a "/" right after it
    ("snmp://10.0.0.1", "snmp://10.0.0.1", True),
    ("snmp://10.0.0.1", "snmp://10.0.0.1/x", True),
    ("snmp://10.0.0.1", "snmp://10.0.0.15", False),
    ("snmp://10.0.0.1", "snmp://10.0.0.1:161", False),
    ("s3://bucket", "s3://bucket/key", True),
    ("s3://bucket", "s3://bucket-evil/key", False),
    ("s3://bucket", "s3://bucket?x=1", False),
    ("dlt://vgi_dlt_fixture", "dlt://vgi_dlt_fixture:shop", False),
    ("dlt://vgi_dlt_fixture", "dlt://vgi_dlt_fixture/shop", True),
    # a scope with anything after the authority stays a plain prefix
    ("s3://bucket/", "s3://bucket/key", True),
    ("s3://bucket/pre", "s3://bucket/prefix/x", True),
    ("s3://bucket/pre", "s3://bucket/other", False),
    ("https://h?a", "https://h?ab", True),
    ("https://h#f", "https://h#fx", True),
    ("a://b\\c", "a://b\\cd", True),
    # empty authority, or not a URI at all: plain prefix
    ("snmp://", "snmp://anything", True),
    ("snmp://10.0.0.", "snmp://10.0.0.15:161", False),
    ("foo", "foobar", True),
    ("c:\\data", "c:\\data2\\x", True),
    ("x:/y://z", "x:/y://zz", True),
    # not a prefix at all
    ("s3://bucket", "s3://buck", False),
    ("", "s3://bucket", False),
]


def test_scope_matches_follows_duckdb() -> None:
    """scope_matches agrees with DuckDB's authority-boundary rule."""
    from vgi.table_function import scope_matches

    for prefix, path, want in _SCOPE_CASES:
        assert scope_matches(prefix, path) is want, (prefix, path, want)


def test_authority_only_scope_does_not_cross_hosts() -> None:
    """An authority-only scope never selects a secret for a neighbouring host or port."""
    s = ResolvedSecrets(
        {
            "host1": {"type": "snmp", "key_id": "h1", "scope": "snmp://10.0.0.1"},
            "host15": {"type": "snmp", "key_id": "h15", "scope": "snmp://10.0.0.15"},
            "default": {"type": "snmp", "key_id": "default"},
        }
    )
    assert _key_id(s.for_scope("snmp://10.0.0.1")) == "h1"
    assert _key_id(s.for_scope("snmp://10.0.0.1/sys")) == "h1"
    assert _key_id(s.for_scope("snmp://10.0.0.15")) == "h15"
    # host:port is not under the authority-only scope: falls back to the unscoped secret
    assert _key_id(s.for_scope("snmp://10.0.0.1:161")) == "default"
    assert _key_id(s.lookup("snmp", scope="snmp://10.0.0.1:161")) == "default"
    assert _key_id(s.lookup("snmp", scope="snmp://10.0.0.1")) == "h1"
