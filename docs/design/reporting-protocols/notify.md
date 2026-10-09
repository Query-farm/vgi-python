# vgi.notify.v1

Python dataclasses and RPC interface: [generated reference](reference/notify.md).
Lifecycle and encoding rules: [wire behavior](wire-contracts.md).

One channel-agnostic protocol for delivering a message to email, Slack,
Microsoft Teams, Discord or a webhook. The caller says *what* to say in a
portable content model; the service decides *how* each channel shows it, and
owns which destinations are allowed. Shared conventions are in
[README.md](README.md#shared-conventions).

## Destinations

Destinations are `{kind, address}` pairs. `kind` is an open set; v1 names these:

| Kind | `address` |
| --- | --- |
| `email` | `ana@example.com` |
| `slack` | Channel or user id, e.g. `C024BE91L` |
| `teams` | Name of a registered channel |
| `discord` | Name of a registered channel |
| `webhook` | Name of a registered endpoint, e.g. `ops-pagerduty`; never a raw URL |

Everything URL-shaped is a **registered destination** the operator configured,
so a schedule can't be pointed at an attacker's URL.

## Message

The fields, defaults and Arrow types are defined by the
[Python dataclasses](reference/notify.md).

Each service maps the model to the channel's native form, and parts a channel
can't carry degrade to text, so callers never branch on channel.

**Attachment URLs** must be fetchable by the notify service: the caller issues
short-lived signed URLs for them. Links (`link_url`) are different: they point
at pages that check the reader's own access.

## Methods

U = unary, S = producer stream. All are required.

| Method | Kind | Purpose |
| --- | --- | --- |
| `get_notify_info(…)` | U | Per kind: enabled, limits (text length, attachment bytes), images, threads; destination policy summary |
| `list_destinations(…)` | S | Registered and discoverable destinations with `display_name`; also address autocomplete |
| `check_destinations(…)` | U | Per destination: allowed, `display_name`, or why it is refused |
| `send(…)` | U | `notification_id` and one result per destination: `accepted`, `refused` (with reason, e.g. `policy`, `unsubscribed`), `failed` (known not accepted), or `unknown` (provider outcome ambiguous) |

`send` delivers to the destinations it can and reports the rest.
`check_destinations` is advisory validation, not an all-or-nothing delivery
guarantee. A caller not on the
sender allowlist gets `action_denied`; a provider outage is
`service_unavailable` with `RetryInfo` only when no destination could have
accepted; otherwise return the per-destination outcomes.

### Delivery and retry outcomes

`accepted` means the service durably accepted responsibility for that
destination, not proof that the recipient received the message. Persist the
request identity, payload and each destination's dispatch state before
provider calls. Concurrent or repeated identical requests within 24 hours
return the original acceptance/result without issuing another delivery.
Reusing the ID for a different payload is `invalid_request`.

If a provider might have accepted a delivery before its response was lost,
record `unknown`. Retry only with provider-supported deduplication or after
reconciling that it did not accept; do not classify ambiguity as a retryable
outage. Queued delivery workers obey the same rule after service acceptance.
Confirmed destinations are never resent when another destination fails.
Callers retain their first-dispatch time and cannot rely on the request ID
after the shared retention window. Exactly-once recipient delivery is not a
protocol guarantee.

## SQL binding

Schema `notify`. Derived by the [shared read-binding rules](README.md#sql-binding).

| Table | List / get |
| --- | --- |
| `destinations` | `list_destinations` |

Read table functions: `get_notify_info`, `list_destinations` and
`check_destinations`. `send` is RPC-only; schedulers, evaluators and task
engines call it using their allowed sender identity.

## Policy

- **Who may send.** An allowlist of principals permitted to call `send`
  (`--notify-senders` / `VGI_NOTIFY_SENDERS`), no permissive default: the
  scheduler's and alert evaluator's service principals, alerting workers. Users
  configure deliveries; those services send them and record `on_behalf_of`.
  Sender authority grants permission to notify only; any separate data access
  requires that identity's explicitly provisioned source delegations. Claimed
  `on_behalf_of` attribution must be authorized by the notify service.
- **Destination policy** is the service's own (email domains, one Slack
  workspace, registered webhooks only), summarized in `get_notify_info` and
  enforced on `send`.
- **Unsubscribe is the notify service's.** It keeps a suppression list keyed by
  address and `correlation` (e.g. `schedule_id`), so an unsubscribe link works
  however the sender is deployed. Suppressed destinations return `refused:
  unsubscribed`.

## Reference notify service (non-normative)

The reference host is a new **`vgi-notify`** repo hosting every channel, with a
stdout channel for development.

- Email (SMTP, SES, Resend) renders HTML and adds one-click `List-Unsubscribe`
  (RFC 8058) through a signed link.
- Slack Block Kit, Teams Adaptive Cards and Discord embeds, through credentials
  held by the service.
- Webhooks send a fixed JSON envelope (`type`, `title`, `summary`, `fields`,
  `link_url`, `correlation`, `sent_at`) signed with the Standard Webhooks
  headers.
- Later: `vgi-email`'s Gmail and Microsoft Graph drivers as an email channel
  (Graph needs the app-only auth in `ENHANCEMENT-microsoft-app-only.md`).
