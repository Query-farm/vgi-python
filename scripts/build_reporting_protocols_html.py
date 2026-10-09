# /// script
# requires-python = ">=3.12"
# dependencies = ["markdown", "pygments"]
# ///
"""Build the reporting design page and separate Python reference pages.

Run after editing a spec: ``uv run --script scripts/build_reporting_protocols_html.py``.
"""

import re
from html import escape
from pathlib import Path

import markdown
from markdown.extensions.toc import slugify
from pygments.formatters import HtmlFormatter

SRC = Path(__file__).resolve().parent.parent / "docs" / "design" / "reporting-protocols"
OUT = SRC / "reporting-protocols.html"
# Page order. Every markdown file in SRC must be listed: an unlisted one fails the
# build rather than silently missing from the page.
FILES = [
    "README.md",
    "python-contracts.md",
    "wire-contracts.md",
    "credentials.md",
    "reports.md",
    "render.md",
    "schedules.md",
    "sql_tasks.md",
    "alerts.md",
    "notify.md",
    "attach-tickets-plan.md",
    "prerequisites.md",
    "IMPLEMENTATION.md",
]
unlisted = sorted({p.name for p in SRC.glob("*.md")} - set(FILES))
if unlisted:
    raise SystemExit(f"add to FILES in {Path(__file__).name}: {', '.join(unlisted)}")

parts = []
for name in FILES:
    text = (SRC / name).read_text()
    if name != "README.md":
        # Demote each spec's H1 to H2 so the whole page has one title.
        text = re.sub(r"^# ", "## ", text, count=1, flags=re.M)
        text = re.sub(r"^(#{2,5}) ", lambda m: "#" + m.group(1) + " ", text.split("\n", 1)[1], flags=re.M)
        first = (SRC / name).read_text().split("\n", 1)[0].replace("# ", "## ", 1)
        text = first + "\n" + text
    parts.append(text)

md_text = "\n\n".join(parts)
# Cross-file links become in-page anchors.
files_alt = "|".join(re.escape(name) for name in FILES)
md_text = re.sub(rf"\]\((?:{files_alt})#([^)]+)\)", r"](#\1)", md_text)
md_text = md_text.replace("](README.md)", "](#top)")
for name in FILES[1:]:
    # A bare link to a file goes to its title, whose anchor the toc extension
    # derives from the H1 text.
    title = (SRC / name).read_text().split("\n", 1)[0].removeprefix("# ")
    md_text = md_text.replace(f"]({name})", f"](#{slugify(title, '-')})")


def render_html(text: str, title: str, home: str) -> str:
    """Render one standalone page, keeping Mermaid outside the highlighter."""
    mermaids: list[str] = []

    def stash(match: re.Match[str]) -> str:
        mermaids.append(match.group(1))
        return f"MERMAIDBLOCK{len(mermaids) - 1}"

    text = re.sub(r"```mermaid\n(.*?)```", stash, text, flags=re.S)
    md = markdown.Markdown(
        extensions=["tables", "fenced_code", "codehilite", "toc", "sane_lists"],
        extension_configs={"codehilite": {"guess_lang": False}, "toc": {"toc_depth": "2-3"}},
    )
    body = md.convert(text)
    for i, diagram in enumerate(mermaids):
        body = body.replace(f"<p>MERMAIDBLOCK{i}</p>", f'<pre class="mermaid">{escape(diagram)}</pre>')
    return HTML_TEMPLATE.format(title=escape(title), home=home, body=body, toc=md.toc, light=light, dark=dark)


light = HtmlFormatter(style="friendly").get_style_defs(".codehilite")
dark = HtmlFormatter(style="github-dark").get_style_defs(".codehilite")
dark = "\n".join("  " + line for line in dark.splitlines())

HTML_TEMPLATE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{title}</title>
<style>
:root {{
  --bg: #fbfbfa; --fg: #1d1d1f; --muted: #5f6368; --rule: #e3e3e0;
  --panel: #f2f2ef; --accent: #2f6f4f; --code-bg: #f4f4f1;
}}
@media (prefers-color-scheme: dark) {{
  :root {{ --bg: #16171a; --fg: #e6e6e3; --muted: #a0a4a8; --rule: #2c2e33;
          --panel: #1e2024; --accent: #7cc49c; --code-bg: #1e2024; }}
}}
* {{ box-sizing: border-box; }}
body {{ margin: 0; background: var(--bg); color: var(--fg);
  font: 16px/1.6 -apple-system, BlinkMacSystemFont, "Segoe UI", Inter, sans-serif; }}
.layout {{ display: grid; grid-template-columns: 280px minmax(0, 1fr); max-width: 1320px; margin: 0 auto; }}
nav {{ position: sticky; top: 0; height: 100vh; overflow-y: auto; padding: 32px 20px;
  border-right: 1px solid var(--rule); font-size: 14px; }}
nav .brand {{ font-weight: 650; margin-bottom: 16px; color: var(--accent); }}
nav ul {{ list-style: none; padding-left: 0; margin: 0; }}
nav ul ul {{ padding-left: 14px; }}
nav li {{ margin: 4px 0; }}
nav a {{ color: var(--muted); text-decoration: none; }}
nav a:hover {{ color: var(--fg); }}
main {{ padding: 40px 56px 96px; min-width: 0; }}
h1 {{ font-size: 2.1rem; line-height: 1.2; margin: 0 0 8px; }}
h2 {{ font-size: 1.55rem; margin-top: 56px; padding-top: 16px; border-top: 1px solid var(--rule); }}
h3 {{ font-size: 1.15rem; margin-top: 32px; }}
a {{ color: var(--accent); }}
code {{ font: 0.88em/1.4 ui-monospace, SFMono-Regular, Menlo, monospace;
  background: var(--code-bg); padding: 1px 5px; border-radius: 4px; }}
pre {{ margin: 0; }}
.codehilite {{ background: var(--code-bg) !important; border: 1px solid var(--rule);
  border-radius: 8px; padding: 14px 16px; overflow-x: auto; margin: 16px 0; }}
.codehilite code {{ background: none; padding: 0; }}
table {{ border-collapse: collapse; width: 100%; margin: 16px 0; font-size: 14.5px;
  display: block; overflow-x: auto; }}
th, td {{ border-bottom: 1px solid var(--rule); padding: 8px 12px; text-align: left; vertical-align: top; }}
th {{ background: var(--panel); font-weight: 600; }}
pre.mermaid {{ background: var(--panel); border-radius: 8px; padding: 16px; text-align: center; }}
@media (max-width: 860px) {{
  .layout {{ grid-template-columns: 1fr; }}
  nav {{ position: static; height: auto; border-right: 0; border-bottom: 1px solid var(--rule); }}
  main {{ padding: 24px 16px 64px; }}
}}
{light}
@media (prefers-color-scheme: dark) {{
{dark}
}}
</style>
</head>
<body>
<div class="layout">
<nav><div class="brand"><a href="{home}">VGI Reporting Protocols</a></div>{toc}</nav>
<main id="top">
{body}
</main>
</div>
<script type="module">
import mermaid from "https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.esm.min.mjs";
const dark = window.matchMedia("(prefers-color-scheme: dark)").matches;
mermaid.initialize({{ startOnLoad: true, theme: dark ? "dark" : "neutral" }});
</script>
</body>
</html>
"""
# The overview links to reference pages; their source is never concatenated here.
md_text = re.sub(r"\]\((reference/[^)#]+)\.md([#][^)]*)?\)", r"](\1.html\2)", md_text)
OUT.write_text(render_html(md_text, "VGI Reporting Protocols", "#top"))
print(OUT)

for reference in sorted((SRC / "reference").glob("*.md")):
    text = reference.read_text()
    for name in FILES:
        title = (SRC / name).read_text().split("\n", 1)[0].removeprefix("# ")
        anchor = "top" if name == "README.md" else slugify(title, "-")
        text = text.replace(f"](../{name})", f"](../{OUT.name}#{anchor})")
        text = text.replace(f"](../{name}#", f"](../{OUT.name}#")
    text = re.sub(r"\]\(([^/)#]+)\.md([#][^)]*)?\)", r"](\1.html\2)", text)
    title = text.split("\n", 1)[0].removeprefix("# ")
    output = reference.with_suffix(".html")
    output.write_text(render_html(text, title, f"../{OUT.name}#python-reporting-contracts"))
    print(output)
