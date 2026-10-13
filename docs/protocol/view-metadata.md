# View creation metadata and column comments

This is part of the unpublished coordinated successor protocol. Native builds
remain on DuckDB 1.5; these contracts do not imply native DuckDB 2 qualification.

`catalog_view_create` now accepts the following metadata together with its
definition, aliases and security mode:

| Field | Wire type | Meaning |
| --- | --- | --- |
| `comment` | nullable UTF-8 | NULL means no comment; an empty string is a present comment. |
| `tags` | nullable map of UTF-8 to UTF-8 | Omission/NULL creates an empty map. Keys and values remain literal strings. |
| `column_comments` | nullable map of UTF-8 to UTF-8 | Keys are raw output identifiers, including literal dots and quotes. Omission/NULL creates an empty map. Values can be empty strings. |

Creation and replacement store the complete supplied metadata. REPLACE resets
omitted metadata; IGNORE preserves the existing object. Invalid output keys
(empty or containing NUL) fail before mutation, including IGNORE and REPLACE.
Providers that cannot preserve requested metadata must reject before effects.
The older Python transactor fixture does this explicitly.

`catalog_view_column_comment_set` takes the attachment, schema path, view name,
`column_path`, nullable `comment`, `ignore_not_found` and optional transaction
handle. `column_path` contains exactly one raw output identifier. A dot in that
component is not a nested path. NULL removes the map entry; an empty string
stores a present empty comment. Missing views obey `ignore_not_found`; invalid
paths still fail. Other view metadata must survive the mutation.

The Python worker applies its existing barrier capability and transaction checks
before this operation. Metadata-only providers do not bind arbitrary SQL to
check output existence; the native DuckDB adapter binds and validates the target
before sending the RPC. Provider query binding, authorization and concurrent
mutation guarantees remain separate obligations.

All seven bundled mutable SDK catalogs preserve creation metadata and support
view and view-column comment changes. Their public RPC test exercises creation,
get/list discovery, IGNORE, REPLACE, empty/NULL comments, malformed paths, missing
views and deletion. The durable Python reference additionally tests sealed HTTP
attachments, reconnect, commit and rollback for regular and barrier views.

Native 1.5 now preserves NULL versus empty view comments and forwards all
`CreateViewInfo` metadata. Its COMMENT routing distinguishes views from tables
and invalidates the affected view cache. A VGI view entry applies explicit
aliases to bound metadata even before the first SELECT, working around 1.5's
initial binding retaining SELECT-derived names. The SQL tests also copy a local
view with comments through `COPY FROM DATABASE ... (SCHEMA)` to exercise creation
metadata without requiring new grammar.

The Go wire `ViewInfo.Comment` is now `*string` to distinguish NULL from empty.
The older static `CatalogView.Comment` descriptor keeps its historical string
zero-value behavior. Rust adds `ViewCreateOptions` and a public column-comment
client method. TypeScript decodes opaque Arrow map parameters before storage.

Remaining view work includes full SDK capability negotiation, owner-relative
query resolution, dependencies, fixed output contracts, concurrent replacement,
and native DuckDB 2 barrier execution. The final request shape and major protocol
version/routing decision remain part of the combined contract review. No
intermediate public release is being made.
