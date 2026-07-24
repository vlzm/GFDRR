"""The quickstart script of the quickstart page, executed verbatim.

The document holds one runnable script (under ``<!-- code:quickstart-script -->``)
and three expected outputs (under ``<!-- output:... -->`` anchors). The page
tells the reader to replace one assignment line and rerun; each replacement
line sits under its own ``<!-- code:...-line -->`` anchor. A test here
extracts the script from the document, applies the same one-line replacement,
executes it, and compares what it prints -- minus the log lines -- with the
document's output block, line by line. If the code changes what the script
prints, these tests go red before the document can drift.
"""

import contextlib
import io
import re
from pathlib import Path

DOC_PATH = (
    Path(__file__).resolve().parent.parent
    / "docs"
    / "site"
    / "content"
    / "docs"
    / "getting-started"
    / "quickstart.md"
)

#: Colour codes in the captured console output.
ANSI_CODE = re.compile(r"\x1b\[[0-9;]*m")
#: A structlog console line starts with a clock time: "16:55:20 [info ] ...".
LOG_LINE = re.compile(r"^\d{2}:\d{2}:\d{2} ")


def read_doc_block(anchor: str) -> str:
    """Parse the fenced block that follows ``<!-- <anchor> -->`` in the document."""
    text = DOC_PATH.read_text(encoding="utf-8")
    marker = f"<!-- {anchor} -->"
    assert marker in text, f"quickstart.md has no anchor {marker}"
    match = re.search(r"```[a-z]*\n(.*?)^```", text.split(marker, 1)[1], re.DOTALL | re.MULTILINE)
    assert match, f"no fenced block under {marker}"
    return match.group(1)


def replace_line(source: str, anchor: str) -> str:
    """Replace one assignment line of the script, the way the page tells the reader to."""
    line = read_doc_block(anchor).strip()
    name = line.split(" =", 1)[0]
    pattern = re.compile(rf"^{name} = .*$", re.MULTILINE)
    assert pattern.search(source), f"the script has no line starting with '{name} = '"
    return pattern.sub(line, source, count=1)


def run_script(source: str) -> list[str]:
    """Execute the script; return the printed lines with the log lines dropped."""
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        exec(compile(source, str(DOC_PATH), "exec"), {"__name__": "__quickstart__"})
    printed = ANSI_CODE.sub("", buffer.getvalue())
    return [line for line in printed.splitlines() if not LOG_LINE.match(line)]


def expected_output(anchor: str) -> list[str]:
    """Parse the document's expected-output block into a list of lines."""
    return read_doc_block(anchor).strip("\n").splitlines()


def test_base_run_prints_the_documented_tables():
    script = read_doc_block("code:quickstart-script")
    assert run_script(script) == expected_output("output:quickstart-base")


def test_full_dock_variant_prints_the_documented_tables():
    script = replace_line(read_doc_block("code:quickstart-script"), "code:quickstart-capacity-line")
    assert run_script(script) == expected_output("output:quickstart-capacity")


def test_stockout_variant_prints_the_documented_tables():
    script = replace_line(
        read_doc_block("code:quickstart-script"), "code:quickstart-inventory-line"
    )
    assert run_script(script) == expected_output("output:quickstart-inventory")
