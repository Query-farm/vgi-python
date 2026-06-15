---
description: "Explanations of how VGI works: the worker lifecycle, transports, the Arrow data model, and argument serialization."
---

# Concepts

**What this is:** explanations of how VGI works under the hood, so you can design workers
correctly and debug them confidently. **Who it's for:** developers who want the "why," not just
the "how." None of this is required to ship your first worker — start with the
[tutorial](../tutorial/index.md) for that.

!!! note "Under construction"
    The concept pages are being expanded (transports, the Arrow data model, catalogs, parallel
    workers). Links below point to existing explanatory material in the meantime.

## Topics

- **Worker lifecycle** — how a call flows through bind → init → process → finish:
  [Function Lifecycle](../lifecycle.md)
- **Argument serialization** — how typed arguments and schemas cross the wire:
  [Argument Serialization](../argument-serialization.md)

## Next steps

- Apply these ideas in the [How-to guides](../how-to/index.md).
- Look up specific classes in the [API Reference](../api/index.md).
