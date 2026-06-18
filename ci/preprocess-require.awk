# Copyright 2025, 2026 Query Farm LLC - https://query.farm
#
# Rewrite each `require <ext>` gate in an upstream vgi sqllogictest into an
# explicit signed INSTALL+LOAD, so the prebuilt standalone `haybarn-unittest`
# (which links none of these extensions) can run the suite. The vgi extension
# comes from the signed community channel; httpfs/json/parquet/spatial from the
# signed core channel. `require-env` and every other directive pass through
# untouched. See ci/README.md.
#
# With `-v http=1`, also inject a signed `INSTALL httpfs FROM core; LOAD httpfs;`
# before the first worker ATTACH (keyed off `require vgi` or `require-env
# VGI_TEST_WORKER`). The prebuilt `haybarn-unittest` does not statically link
# httpfs, so `ATTACH ... (TYPE vgi, LOCATION 'http://...')` fails with a binder
# error unless httpfs is loaded into the connection first. Upstream's own
# `unittest` build links httpfs, so its `make test_http` needs no such step.
BEGIN { injected = 0 }
function inject_httpfs() {
    if (http != 1 || injected) return
    print "";
    print "statement ok"; print "INSTALL httpfs FROM core;"; print "";
    print "statement ok"; print "LOAD httpfs;";
    injected = 1
}
/^require[ \t]+vgi[ \t]*$/ {
    print "statement ok"; print "INSTALL vgi FROM community;"; print "";
    print "statement ok"; print "LOAD vgi;";
    inject_httpfs();
    next
}
/^require[ \t]+(httpfs|json|parquet|spatial)[ \t]*$/ {
    ext = $2
    print "statement ok"; print "INSTALL " ext " FROM core;"; print "";
    print "statement ok"; print "LOAD " ext ";"; next
}
/^require-env[ \t]+VGI_TEST_WORKER[ \t]*$/ {
    print
    inject_httpfs();
    next
}
{ print }
