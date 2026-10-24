# vgi.notify.v1

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

```python
@dataclass
class Notification:
    title: str                            # subject line / headline
    summary: str                          # plain text, always delivered
    body_markdown: str = ""               # CommonMark subset; converted per channel
    link_url: str = ""                    # primary action, e.g. the run or alert page
    link_label: str = ""
    fields: list[Field] = []              # headline values: {label, value}
    image: Attachment = ...               # inline preview; empty url = none
    attachments: list[Attachment] = []    # {filename, media_type, url, size_bytes, sha256}
    severity: str = "info"                # info | warning | critical
    thread_key: str = ""                  # groups follow-ups (Slack thread, email In-Reply-To)
    on_behalf_of: PrincipalRef = ...
    correlation: dict[str, str] = {}      # schedule_id, run_id, rule_id, ...
```

Each service maps the model to the channel's native form, and parts a channel
can't carry degrade to text, so callers never branch on channel.

**Attachment URLs** must be fetchable by the notify service: the caller issues
short-lived signed URLs for them. Links (`link_url`) are different: they point
at pages that check the reader's own access.

## Methods

U = unary, S = producer stream. All are required.

| Method | Kind | Purpose |
| --- | --- | --- |
| `get_notify_info()` | U | Per kind: enabled, limits (text length, attachment bytes), images, threads; destination policy summary |
| `list_destinations(kind, query)` | S | Registered and discoverable destinations with `display_name`; also address autocomplete |
| `check_destinations(destinations)` | U | Per destination: allowed, `display_name`, or why it is refused |
| `send(request_id, destinations, notification)` | U | `notification_id` and one result per destination: `accepted`, `refused` (with reason, e.g. `policy`, `unsubscribed`) or `failed` |

`send` delivers to the destinations it can and reports the rest. A caller that
wants all-or-nothing calls `check_destinations` first. A caller not on the
sender allowlist gets `action_denied`; a provider outage is
`service_unavailable` with `RetryInfo`.

## SQL binding

Schema `notify`. Derived by the rules in [README.md](README.md#sql-binding); this is the annotation.

| Table | List / get | `INSERT` | `UPDATE` | `DELETE` |
| --- | --- | --- | --- | --- |
| `destinations` | `list_destinations` | | | |

`send` and `check_destinations` are procedures, so a SQL task can notify as a
step of its script.

## Policy

- **Who may send.** An allowlist of principals permitted to call `send`
  (`--notify-senders` / `VGI_NOTIFY_SENDERS`), no permissive default: the
  scheduler's and alert evaluator's service principals, alerting workers. Users
  configure deliveries; those services send them and record `on_behalf_of`.
  These principals send messages; they never read data.
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
