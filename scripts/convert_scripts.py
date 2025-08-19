#!/usr/bin/env python3
"""
Dual-mode converter:
- URL mode: fetch a page that contains ALL buyer types and split into .md files.
- DOCX mode: parse one or more Word .docx files and write .md files.

Usage (from repo root):

DOCX (single file or glob):
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

# Optional DOCX parsers (we'll use whichever is available)
HAVE_MAMMOTH = False
HAVE_PYDOCX = False
try:
    import mammoth  # type: ignore
    HAVE_MAMMOTH = True
except Exception:
    HAVE_MAMMOTH = False
try:
    import docx  # type: ignore
    HAVE_PYDOCX = True
except Exception:
    HAVE_PYDOCX = False

# ---------------- CLI ----------------
parser = argparse.ArgumentParser(description="Convert sales scripts (URL or DOCX) to Markdown library files.")
parser.add_argument("--url", help="Source page containing all buyer sections")
parser.add_argument("--docx", help="Glob path OR exact .docx file path (uses python-docx or mammoth)")
parser.add_argument("--buyer", help="Force buyer id (innovator|early-adopter|early-majority|late-majority|sceptic)")
parser.add_argument("--out", required=True, help="Output dir, e.g. web/content/call-library/v1/direct/connectivity")
args = parser.parse_args()
OUT_DIR = Path(args.out).resolve()

# ------------- Constants -------------
REQUIRED_HEADINGS = [
    "Opener",
    "Context bridge",
    "Value moment",
    "Exploration nudge",
    "Objections",
    "Next step (salesperson-chosen)",
    "Close",
]

# Canonical mapping & sets
BUYER_MAP = {
    # Innovator
    "innovator": "innovator",
    "innovators": "innovator",

    # Early Adopter
    "early adopter": "early-adopter",
    "early adopters": "early-adopter",
    "early-adopter": "early-adopter",
    "early-adopters": "early-adopter",
    "early_adopter": "early-adopter",
    "early_adopters": "early-adopter",

    # Early Majority
    "early majority": "early-majority",
    "early-majority": "early-majority",
    "early_majority": "early-majority",

    # Late Majority
    "late majority": "late-majority",
    "late-majority": "late-majority",
    "late_majority": "late-majority",

    # Sceptic (tolerate US spelling)
    "sceptic": "sceptic",
    "sceptics": "sceptic",
    "skeptic": "sceptic",
    "skeptics": "sceptic",
}
VALID_BUYERS = {"innovator", "early-adopter", "early-majority", "late-majority", "sceptic"}

# Flexible input → canonical section mapping
SECTION_RULES = 

[
    (re.compile(r"^\s*(\d+\s*\|\s*)?(opening|opener)\b", re.I), "Opener", False),
    (re.compile(r"^\s*(\d+\s*\|\s*)?(buyer\s+pain|pain)\b", re.I), "Context bridge", False),
    (re.compile(r"^\s*(\d+\s*\|\s*)?(buyer\s+desire|desire)\b", re.I), "Value moment", False),
    (re.compile(r"^\s*(\d+\s*\|\s*)?example", re.I), "Value moment", True),
    (re.compile(r"^\s*(\d+\s*\|\s*)?(handling\s+objections|objections)\b", re.I), "Objections", False),
    (re.compile(r"^\s*(\d+\s*\|\s*)?(call\s+to\s+action|next\s*steps?)\b", re.I), "Next step", False),
    (re.compile(r"^\s*(\d+\s*\|\s*)?(exploration|discovery|questions?)\b", re.I), "Exploration nudge", False),
]

# ------------- Utilities -------------
def ensure_out() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

def _norm(s: str) -> str:
    """Lowercase; collapse spaces/underscores/hyphens to a single dash."""
    return re.sub(r"[\s_\-]+", "-", (s or "").lower())

def _tokenise_filename_for_buyer(path: str) -> List[str]:
    """
    Tokenises filename into simple words (removing extension and punctuation) for robust buyer match.
    """
    base = os.path.basename(path)
    stem = os.path.splitext(base)[0]  # drop .docx
    # replace separators with spaces, then split
    cleaned = re.sub(r"[_\-\.\s]+", " ", stem.lower())
    return cleaned.split()

def detect_buyer_from_filename(path: str) -> str:
    tokens = _tokenise_filename_for_buyer(path)
    joined = " ".join(tokens)
    # Strong contains match
    for key, val in BUYER_MAP.items():
        if key in joined:
            return val
    # Token combos
    s = set(tokens)
    def has(*ws): return all(w in s for w in ws)
    if has("innovator") or has("innovators"):
        return "innovator"
    if has("early","adopter") or has("early","adopters"):
        return "early-adopter"
    if has("early","majority"):
        return "early-majority"
    if has("late","majority"):
        return "late-majority"
    if has("sceptic") or has("sceptics") or has("skeptic") or has("skeptics"):
        return "sceptic"
    return ""

def html_to_text(html: str) -> str:
    """Loose HTML → plain text normaliser."""
    if not html:
        return ""
    html = re.sub(r"(?is)<li[^>]*>\s*", "\n- ", html)
    html = re.sub(r"(?is)<br\s*/?>", "\n", html)
    html = re.sub(r"(?is)</(p|div|section|ul|ol|h[1-6])>", "\n\n", html)
    html = re.sub(r"(?is)<[^>]+>", "", html)
    text = htmllib.unescape(html)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()

def compose_markdown(sections: Dict[str, str]) -> str:
    """
    Build final Markdown ensuring:
      - 7 required headings present
      - {{value_proposition}} inside Value moment
      - {{next_step}} inside Next step (salesperson-chosen)
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

    if out["Value moment"]:
        if "{{value_proposition}}" not in out["Value moment"]:
            out["Value moment"] += "\n\n{{value_proposition}}"
    else:
        out["Value moment"] = "{{value_proposition}}"

    if out["Next step (salesperson-chosen)"]:
        if "{{next_step}}" not in out["Next step (salesperson-chosen)"]:
            out["Next step (salesperson-chosen)"] += "\n\n{{next_step}}"
    else:
        out["Next step (salesperson-chosen)"] = "{{next_step}}"

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
    file_path = OUT_DIR / f"{buyer_id}.md"
    file_path.write_text(md, encoding="utf-8")
    print(f"✓ Wrote {file_path}")

def canonical_buyer(s: str) -> str:
    if not s:
        return ""
    q = s.strip().lower().replace("_", " ").replace("-", " ")
    q = re.sub(r"\s+", " ", q)
    return BUYER_MAP.get(q, s.strip().lower())

def guess_buyer_from_body(text: str) -> str:
    low = text.lower()
    # Fast contains checks
    for key, val in BUYER_MAP.items():
        if re.search(rf"\b{re.escape(key)}\b", low):
            return val
    # Token combos
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
    if any(w in tokens for w in ["sceptic","sceptics","skeptic","skeptics"]):
        return "sceptic"
    return ""

def map_inner_sections_to_canonical(text: str) -> Dict[str, str]:
    """
    Group lines into canonical sections using tolerant regex rules.
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
        matched = False
        for pattern, target, _append in SECTION_RULES:
            if pattern.match(line):
                current = target
                buckets.setdefault(current, [])
                matched = True
                break
        if not matched:
            push(line)

    out: Dict[str, str] = {k: "\n".join(v).strip() for k, v in buckets.items()}
    for h in REQUIRED_HEADINGS:
        out.setdefault(h, "")
    return out

# ------------- URL MODE -------------
def run_url_mode(url: str) -> None:
    if not HAVE_BS:
        print("URL mode requires BeautifulSoup + html5lib. Install:\n  pip install beautifulsoup4 html5lib")
        sys.exit(1)

    print(f"Fetching: {url}")
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req) as resp:
        raw_html = resp.read().decode("utf-8", errors="ignore")
    soup = BeautifulSoup(raw_html, "html5lib")

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

    seen, anchors_ordered = set(), []
    for val, el in anchors:
        if val not in seen and val in VALID_BUYERS:
            seen.add(val)
            anchors_ordered.append((val, el))

    for i, (buyer, el) in enumerate(anchors_ordered):
        stop_ids = {id(x[1]) for x in anchors_ordered[i + 1:]}
        parts, cur = [], el.find_next_sibling()
        while cur is not None and id(cur) not in stop_ids:
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

# ------------- DOCX MODE -------------
def docx_to_text_mammoth(path: str) -> str:
    with open(path, "rb") as fh:
        result = mammoth.convert_to_html(fh)
        html = result.value or ""
    return html_to_text(html)

def docx_to_text_python_docx(path: str) -> str:
    d = docx.Document(path)
    paras = []
    for p in d.paragraphs:
        t = p.text.strip()
        if t:
            paras.append(t)
    return "\n\n".join(paras)

def run_docx_mode(pattern: str, buyer_override: str | None) -> None:
    # Accept exact file or glob
    files = [pattern] if os.path.isfile(pattern) else sorted(glob.glob(pattern))
    if not files:
        print(f"✗ No .docx files matched: {pattern}")
        sys.exit(1)

    if not (HAVE_MAMMOTH or HAVE_PYDOCX):
        print("DOCX mode needs one of:\n  pip install python-docx\nor\n  pip install mammoth")
        sys.exit(1)

    for f in files:
        print(f"Reading {f}")

        # 1) Decide buyer up-front from filename (deterministic)
        buyer = canonical_buyer(buyer_override) if buyer_override else ""
        matched_source = "(forced)" if buyer_override else ""

        if not buyer:
            buyer = detect_buyer_from_filename(f)
            matched_source = "filename" if buyer else ""

        # 2) Parse text (we’ll still read body for section mapping)
        origin = None
        text = ""
        try:
            if HAVE_PYDOCX:
                text = docx_to_text_python_docx(f)
                origin = "python-docx"
            elif HAVE_MAMMOTH:
                text = docx_to_text_mammoth(f)
                origin = "mammoth"
        except Exception as e:
            print(f"  ! Failed to parse DOCX: {e}")
            # still proceed if we already have buyer and at least create a skeleton
            text = ""

        # 3) If buyer still unknown, try the body text
        if not buyer:
            body_guess = guess_buyer_from_body(text)
            if body_guess:
                buyer = body_guess
                matched_source = "body"

        # 4) Finalise buyer decision
        if not buyer:
            buyer = "early-majority"
            matched_source = "default"
            print("  · detect -> none, defaulting to 'early-majority'")
        elif buyer not in VALID_BUYERS:
            print(f"  ! Skipping (buyer not recognised): {f} (got '{buyer}')")
            continue

        print(f"  · detect -> buyer='{buyer}' via {matched_source}; parser={origin or 'n/a'}")

        # 5) Map sections & compose
        if text.strip():
            sections = map_inner_sections_to_canonical(text)
        else:
            sections = {h: "" for h in REQUIRED_HEADINGS}  # empty skeleton

        md = compose_markdown(sections)
        if origin:
            md += f"\n<!-- Parsed via {origin} -->\n"
        write_md(buyer, md)

# ------------- main -------------
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
