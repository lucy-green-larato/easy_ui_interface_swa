#!/usr/bin/env python3
"""
Dual-mode converter:
- URL mode: fetch a page that contains ALL buyer types and split into .md files.
- DOCX mode: parse one or more Word .docx files and write .md files.

Usage (from repo root):

DOCX mode (single file or glob):
  python scripts/convert_scripts.py --docx "script_doc_inputs/connectivity-early-adopters.docx" \
    --out web/content/call-library/v1/direct/connectivity

Force buyer if detection is fussy:
  python scripts/convert_scripts.py --docx "script_doc_inputs/connectivity-early-adopters.docx" \
    --buyer early-adopter \
    --out web/content/call-library/v1/direct/connectivity

URL mode:
  python scripts/convert_scripts.py --url "https://info.larato.co.uk/xxx-sales-script" \
    --out web/content/call-library/v1/direct/connectivity
"""

import argparse
import glob
import os
import re
import sys
import urllib.request
import html as htmllib
from pathlib import Path
from typing import Dict, Tuple, List

# Optional (URL mode): pip install beautifulsoup4 html5lib
try:
    from bs4 import BeautifulSoup  # noqa: F401
    HAVE_BS = True
except Exception:
    HAVE_BS = False

# Optional (DOCX mode) parsers:
# 1) mammoth (preferred HTML fidelity), 2) python-docx fallback (no external HTML).
_HAVE_MAMMOTH = False
_HAVE_PYDOCX = False
try:
    import mammoth  # type: ignore
    _HAV E_MAMMOTH = True
except Exception:
    _HAVE_MAMMOTH = False
try:
    import docx  # type: ignore
    _HAVE_PYDOCX = True
except Exception:
    _HAVE_PYDOCX = False

# --------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------
parser = argparse.ArgumentParser(description="Convert sales scripts (URL or DOCX) to Markdown library files.")
parser.add_argument("--url", help="Source page containing all buyer sections")
parser.add_argument("--docx", help="Glob path OR exact .docx file path (uses mammoth if available, else python-docx)")
parser.add_argument("--buyer", help="Force buyer id (innovator|early-adopter|early-majority|late-majority|sceptic)")
parser.add_argument("--out", required=True, help="Output dir, e.g. web/content/call-library/v1/direct/connectivity")
args = parser.parse_args()

OUT_DIR = Path(args.out).resolve()

# --------------------------------------------------------------------
# Canonical mapping & sets
# --------------------------------------------------------------------
BUYER_MAP = {
    # Innovator
    "innovators": "innovator",
    "innovator": "innovator",

    # Early Adopter
    "early adopters": "early-adopter",
    "early adopter": "early-adopter",
    "early_adopters": "early-adopter",
    "early-adopters": "early-adopter",

    # Early Majority
    "early majority": "early-majority",
    "early_majority": "early-majority",
    "early-majority": "early-majority",

    # Late Majority
    "late majority": "late-majority",
    "late_majority": "late-majority",
    "late-majority": "late-majority",

    # Sceptic (tolerate US spelling)
    "sceptics": "sceptic",
    "sceptic": "sceptic",
    "skeptics": "sceptic",
    "skeptic": "sceptic",
}
VALID_BUYERS = {"innovator", "early-adopter", "early-majority", "late-majority", "sceptic"}

REQUIRED_HEADINGS = [
    "Opener",
    "Context bridge",
    "Value moment",
    "Exploration nudge",
    "Objections",
    "Next step (salesperson-chosen)",
    "Close",
]

# Flexible input → canonical section mapping
SECTION_RULES: List[Tuple[re.Pattern, str, bool]] = [
    (re.compile(r"^\s*(opening|opener)\b", re.I), "Opener", False),
    (re.compile(r"^\s*(buyer\s+pain|pain)\b", re.I), "Context bridge", False),
    (re.compile(r"^\s*(buyer\s+desire|desire)\b", re.I), "Value moment", False),
    (re.compile(r"^\s*example", re.I), "Value moment", True),
    (re.compile(r"^\s*(handling\s+objections|objections)\b", re.I), "Objections", False),
    (re.compile(r"^\s*(call\s+to\s+action|next\s*steps?)\b", re.I), "Next step (salesperson-chosen)", False),
    (re.compile(r"^\s*(exploration|discovery|questions?)\b", re.I), "Exploration nudge", False),
    # Also accept exact required headings even if they already exist in source
    (re.compile(r"^\s*#?\s*opener\s*$", re.I), "Opener", False),
    (re.compile(r"^\s*#?\s*context\s*bridge\s*$", re.I), "Context bridge", False),
    (re.compile(r"^\s*#?\s*value\s*moment\s*$", re.I), "Value moment", False),
    (re.compile(r"^\s*#?\s*exploration\s*nudge\s*$", re.I), "Exploration nudge", False),
    (re.compile(r"^\s*#?\s*objections\s*$", re.I), "Objections", False),
    (re.compile(r"^\s*#?\s*next\s*step\s*\(salesperson-chosen\)\s*$", re.I), "Next step (salesperson-chosen)", False),
    (re.compile(r"^\s*#?\s*close\s*$", re.I), "Close", False),
]

# --------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------
def ensure_out() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

def _norm(s: str) -> str:
    """Lowercase; collapse spaces/underscores/hyphens to a single dash."""
    return re.sub(r"[\s_\-]+", "-", (s or "").lower())

def html_to_text(html: str) -> str:
    """Very tolerant HTML → plain text normaliser."""
    if not html:
        return ""
    # Convert lists and line breaks
    html = re.sub(r"(?is)<li[^>]*>\s*", "\n- ", html)
    html = re.sub(r"(?is)<br\s*/?>", "\n", html)
    html = re.sub(r"(?is)</(p|div|section|ul|ol|h[1-6])>", "\n\n", html)
    # Strip tags
    html = re.sub(r"(?is)<[^>]+>", "", html)
    # Unescape entities
    text = htmllib.unescape(html)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()

def compose_markdown(sections: Dict[str, str]) -> str:
    """
    Build final Markdown with required headings, guaranteeing tokens:
    - {{value_proposition}} included in "Value moment" (woven, not dumped elsewhere)
    - {{next_step}} included in "Next step (salesperson-chosen)"
    - Close ends with "Thank you for your time."
    """
    out = {
        "Opener": sections.get("Opener", "").strip(),
        "Context bridge": sections.get("Context bridge", "").strip(),
        "Value moment": sections.get("Value moment", "").strip(),
        "Exploration nudge": sections.get("Exploration nudge", "").strip(),
        "Objections": sections.get("Objections", "").strip(),
        "Next step (salesperson-chosen)": sections.get("Next step (salesperson-chosen)", "").strip(),
        "Close": sections.get("Close", "").strip(),
    }

    # Value moment must weave {{value_proposition}}
    if out["Value moment"]:
        if "{{value_proposition}}" not in out["Value moment"]:
            out["Value moment"] += "\n\n{{value_proposition}}"
    else:
        out["Value moment"] = "{{value_proposition}}"

    # Next step must include {{next_step}}
    if out["Next step (salesperson-chosen)"]:
        if "{{next_step}}" not in out["Next step (salesperson-chosen)"]:
            out["Next step (salesperson-chosen)"] += "\n\n{{next_step}}"
    else:
        out["Next step (salesperson-chosen)"] = "{{next_step}}"

    # Close must end correctly
    close = out["Close"].strip()
    if not close:
        close = "Thank you for your time."
    elif not re.search(r"thank you for your time\.\s*$", close, re.I):
        close = close.rstrip() + "\n\nThank you for your time."
    out["Close"] = close

    order = [
        "Opener",
        "Context bridge",
        "Value moment",
        "Exploration nudge",
        "Objections",
        "Next step (salesperson-chosen)",
        "Close",
    ]
    return "\n\n".join(f"# {h}\n{out[h].rstrip()}" for h in order).strip() + "\n"

def write_md(buyer_id: str, md: str) -> None:
    ensure_out()
    buyer_id = canonical_buyer(buyer_id)
    file_path = OUT_DIR / f"{buyer_id}.md"
    file_path.write_text(md, encoding="utf-8")
    print(f"✓ Wrote {file_path}")

def canonical_buyer(s: str) -> str:
    if not s:
        return ""
    q = s.strip().lower()
    q = q.replace("_", "-").replace("  ", " ")
    q = re.sub(r"\s+", " ", q)
    # direct mapping first
    if q in BUYER_MAP:
        return BUYER_MAP[q]
    # contains match
    for key, val in BUYER_MAP.items():
        if key in q:
            return val
    # already canonical?
    if q in VALID_BUYERS:
        return q
    # last fallback
    return q

def guess_buyer_from_filename(path: str) -> str:
    base = os.path.basename(path).lower()
    norm = base.replace("_", " ").replace("-", " ")
    for key, val in BUYER_MAP.items():
        if key in base or key in norm:
            return val
    return ""

def guess_buyer_from_body(text: str) -> str:
    low = text.lower()
    for key, val in BUYER_MAP.items():
        if re.search(rf"\b{re.escape(key)}\b", low):
            return val
    # token combo fallbacks
    tokens = set(re.split(r"[^a-z0-9]+", low))
    def has(*ws): return all(w in tokens for w in ws)
    if has("innovator") or has("innovators"):
        return "innovator"
    if has("early","adopter") or has("early","adopters"):
        return "early-adopter"
    if has("early","majority"):
        return "early-majority"
    if has("late","majority"):
        return "late-majority"
    if any(has(x) for x in [["sceptic"],["sceptics"],["skeptic"],["skeptics"]]):
        return "sceptic"
    return ""

def map_inner_sections_to_canonical(text: str) -> Dict[str, str]:
    """
    Take a rough, plain-text block and group lines into canonical sections
    using SECTION_RULES + tolerant heading detection.
    """
    lines = [l.rstrip() for l in text.splitlines()]
    buckets: Dict[str, List[str]] = {}
    current = "Opener"
    buckets[current] = []

    def push(line: str) -> None:
        if line.strip():
            buckets.setdefault(current, []).append(line)

    for raw in lines:
        line = raw.strip()
        if not line:
            continue

        # Detect explicit required headings like "Opener", "# Opener", etc.
        matched = False
        for pattern, target, _append in SECTION_RULES:
            if pattern.match(line):
                current = target
                buckets.setdefault(current, [])
                matched = True
                break

        if not matched:
            push(line)

    # Join
    out: Dict[str, str] = {}
    for k, arr in buckets.items():
        out[k] = "\n".join(arr).strip()

    # Ensure all required keys exist
    for h in REQUIRED_HEADINGS:
        out.setdefault(h, "")

    return out

# --------------------------------------------------------------------
# URL MODE
# --------------------------------------------------------------------
def run_url_mode(url: str) -> None:
    if not HAVE_BS:
        print("URL mode requires BeautifulSoup + html5lib. Install with:\n  pip install beautifulsoup4 html5lib")
        sys.exit(1)

    print(f"Fetching: {url}")
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req) as resp:
        raw_html = resp.read().decode("utf-8", errors="ignore")
    soup = BeautifulSoup(raw_html, "html5lib")

    # Find anchor elements for each buyer
    anchors: List[Tuple[str, object]] = []
    for tag in soup.find_all(True):
        txt = (tag.get_text(" ", strip=True) or "").lower()
        ident = " ".join([tag.get("id", ""), " ".join(tag.get("class") or [])]).lower()
        candidate = f"{txt} {ident}".strip()
        for key, val in BUYER_MAP.items():
            if key in candidate:
                anchors.append((val, tag))

    if not anchors:
        print("✗ Could not detect buyer sections on the page.")
        sys.exit(1)

    # Keep first instance per buyer in page order
    seen, anchors_ordered = set(), []
    for val, el in anchors:
        if val not in seen and val in VALID_BUYERS:
            seen.add(val)
            anchors_ordered.append((val, el))

    # Extract content between anchors
    for i, (buyer, el) in enumerate(anchors_ordered):
        stop_set = {id(x[1]) for x in anchors_ordered[i + 1:]}
        parts, cur = [], el.find_next_sibling()
        while cur is not None and id(cur) not in stop_set:
            parts.append(str(cur))
            cur = cur.find_next_sibling()
        seg_html = "".join(parts)
        if not seg_html.strip():
            print(f"! Buyer '{buyer}' found but segment empty.")
            continue

        text = html_to_text(seg_html)
        sections = map_inner_sections_to_canonical(text)
        md = compose_markdown(sections)
        write_md(buyer, md)

# --------------------------------------------------------------------
# DOCX MODE
# --------------------------------------------------------------------
def docx_to_html_text_mammoth(path: str) -> str:
    with open(path, "rb") as fh:
        result = mammoth.convert_to_html(fh)
        html = result.value or ""
    return html

def docx_to_text_python_docx(path: str) -> str:
    d = docx.Document(path)
    paras = []
    for p in d.paragraphs:
        t = p.text.strip()
        if t:
            paras.append(t)
    # Add simple blank lines between paragraphs
    return "\n\n".join(paras)

def run_docx_mode(pattern: str, buyer_override: str | None) -> None:
    # Accept BOTH a glob and an exact file path
    if os.path.isfile(pattern):
        files = [pattern]
    else:
        files = sorted(glob.glob(pattern))

    if not files:
        print(f"✗ No .docx files matched: {pattern}")
        sys.exit(1)

    if not (_HAVE_MAMMOTH or _HAVE_PYDOCX):
        print("DOCX mode needs one of:\n  pip install mammoth\nor\n  pip install python-docx")
        sys.exit(1)

    for f in files:
        print(f"Reading {f}")
        try:
            if _HAVE_MAMMOTH:
                html = docx_to_html_text_mammoth(f)
                text = html_to_text(html)
                origin = "mammoth"
            else:
                text = docx_to_text_python_docx(f)
                origin = "python-docx"
        except Exception as e:
            print(f"  ! Failed to parse DOCX with available parsers: {e}")
            continue

        buyer = canonical_buyer(buyer_override) if buyer_override else ""
        matched_key = "(forced)" if buyer_override else None

        if not buyer:
            # Robust buyer detection: filename OR document body
            nf = _norm(os.path.basename(f))   # e.g. connectivity-early-adopters.docx
            nt = _norm(text)

            for key, val in BUYER_MAP.items():
                nk = _norm(key)
                if nk in nf or nk in nt:
                    buyer = val
                    matched_key = key
                    break

            if not buyer:
                buyer = guess_buyer_from_body(text)

        if not buyer:
            buyer = "early-majority"  # practical default
            print("  · detect -> (none) => defaulting to 'early-majority'")
        else:
            print(f"  · detect -> file='{os.path.basename(f)}' | match='{matched_key}' => buyer='{buyer}'")

        if buyer not in VALID_BUYERS:
            print(f"  ! Skipping (buyer not recognised): {f}  (got '{buyer}')")
            continue

        sections = map_inner_sections_to_canonical(text)
        md = compose_markdown(sections)
        md += f"\n<!-- Parsed via {origin} -->\n"
        write_md(buyer, md)

# --------------------------------------------------------------------
# main
# --------------------------------------------------------------------
def main() -> None:
    if not args.url and not args.docx:
        print("Provide either --url or --docx")
        sys.exit(1)

    ensure_out()

    if args.url:
        run_url_mode(args.url)

    if args.docx:
        run_docx_mode(args.docx, args.buyer)

    print("Done.")

if __name__ == "__main__":
    main()
