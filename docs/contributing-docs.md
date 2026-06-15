---
description: "Conventions for writing vgi-python documentation: the per-page orientation standard, example rules, and a page template."
---

# Writing docs

**What this is:** the conventions every vgi-python documentation page follows.<br>
**Who it's for:** anyone adding or editing docs. Following these keeps the site easy to orient in
and keeps examples from rotting.

## Diátaxis: pick the right mode

Every page belongs to exactly one of four modes. If a page is doing two jobs, split it.

| Mode | Answers | Lives under |
|---|---|---|
| **Tutorial** | "Teach me, step by step, by doing." | `docs/tutorial/` |
| **How-to** | "How do I accomplish task X?" | `docs/how-to/` |
| **Concept** | "Why does it work this way?" | `docs/concepts/` |
| **Reference** | "What exactly is the signature/contract?" | `docs/api/` (auto-generated) |

## The per-page orientation standard

Every tutorial, how-to, and concept page **must** contain, in order:

1. **A lead "what + who" block** — one sentence on what the page is, then **on its own line**
   (use a trailing `<br>`) one phrase on who it's for, so a reader self-orients in under 10 seconds.
   (See the top of this page.)
2. **Prerequisites** — assumed knowledge, prior steps, and required extras (e.g.
   `pip install vgi-python[http]`), with links. Use a list or an admonition.
3. **At least one complete, runnable example** — no `...` elisions in the primary example. It must
   pass the documentation-example tests (see below). *Exception:* advanced pages whose feature
   isn't exercisable from a self-contained snippet (HTTP serving, auth, optimizer pushdown) may
   lead with an illustrative `test="skip"` sketch — but label it illustrative and point to a
   runnable worker or the reference for the real thing.
4. **A "Next steps" section** that advances the reader along the funnel: prefer a sibling **how-to**
   or a **concept** page, then the **reference** for the full contract. Don't jump straight from a
   how-to into auto-generated reference. No dead ends.

## Example rules

- **Prefer one source of truth.** Worker code lives in `examples/*.py` and is embedded with a
  snippet so docs and tests share one file:

  ```text
  ```python
  --8<-- "examples/calc_worker.py"
  ```
  ```

- **Examples must run in CI.** `examples/*.py` are imported and exercised by the test suite;
  inline (non-snippet) Python blocks are executed by `tests/test_documentation_examples.py`. A
  broken example fails the build.
- **Mark illustrative-only blocks.** If a block genuinely can't run standalone (SQL, shell, a
  partial snippet), use a non-`python` fence or the ` ```python test="lint" ` / ` test="skip" `
  setting so the harness lints but doesn't execute it. **Do not put blank lines inside a
  `test="skip"` block** — the renderer doesn't own that info string, so a blank line splits the
  fence and leaks the delimiters into the page. Keep skip snippets blank-line-free.
- **Progressive disclosure for newcomers.** Put background that experts can skip inside a
  collapsible admonition (`??? info "New to X?"`).

## Page template

Copy this skeleton when starting a new how-to or concept page:

```markdown
---
description: "One-line summary used for SEO and search."
---

# Page title

**What this is:** one sentence.<br>
**Who it's for:** one phrase.

## Prerequisites

- ...

## <the steps / the explanation>

```python
--8<-- "examples/your_worker.py"
```

## Next steps

- [Related page](...)
```
