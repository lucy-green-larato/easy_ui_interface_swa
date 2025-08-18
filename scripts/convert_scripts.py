#!/usr/bin/env python3
"""
Dual-mode converter:
- URL mode: fetch a page that contains ALL buyer types and split into .md files.
- DOCX mode: parse one or more Word .docx files and write .md files.

Usage (from repo root):

DOCX mode:
  python scripts/convert_scripts.py --docx "script_doc_inputs/*.docx" --out web/content/call-library/v1/direct/connectivity
  # or a single file:
  python scripts/convert_scripts.py --docx script_doc_inputs/connectivity-early-adopters.docx --out web/content/call-library/v1/direct/connectivity

URL mode:
  python scripts/convert_scripts.py --url "https://info.larato.co.uk/xxx-sales-script" --out web/content/call-library/v1/direct/connectivity
"""

import argparse, glob, os, re, sys, urllib.request, html as htmllib
from pathlib import Path

# ---- optional for URL mode (install once: pip install beautifulsoup4 html5lib) ----
try:
    from bs4 import BeautifulSoup  # noqa: F401
    HAVE_BS = True
except Exception:
    HAVE_BS = False

parser = argparse.ArgumentParser(description="Convert sales scripts (URL or DOCX) to Markdown library files.")
parser.add_argument("--url", help="Source page containing all buyer sections")
parser.add_argument("--docx", help="Glob path OR exact .docx file path (requires mammoth)")
parser.add_argument("--out", required=True, help="Output dir, e.g. web/content/call-library/v1/direct/connectivity")
args = parser.parse_args()

OUT_DIR = Path(args.out)

# ---------- canonical mapping ----------
BUYER_MAP = {
    "innovators": "innovator",
    "innovator": "innovator",
    "early adopters": "early-adopter",
    "early adopter": "early-adopter",
    "early_adopters": "early-adopter",   # alias
    "early-adopters": "early-adopter",   # alias
    "early majority": "early-majority",
    "late majority": "late-majority",
    "sceptic": "sceptic",
    "sceptics": "sceptic",
    "skeptic": "sceptic",
    "skeptics": "sceptic",
}
VALID_BUYERS = {"innovator","early-adopter","early-majority","late-majority","sceptic"}

SECTION_RULES = [
    (re.compile(r"^\s*(opening|opener)\b", re.I), "Opener", False),
    (re.compile(r"^\s*(buyer\s+pain|pain)\b", re.I), "Context bridge", False),
    (re.compile(r"^\s*(buyer\s+desire|desire)\b", re.I), "Value moment", False),
    (re.compile(r"^\s*example", re.I), "Value moment", True),
    (re.compile(r"^\s*(handling\s+objections|objections)\b", re.I), "Objections", False),
    (re.compile(r"^\s*(call\s+to\s+action|next\s*steps?)\b", re.I), "Next step", False),
    (re.compile(r"^\s*(exploration|discovery|questions?)\b", re.I), "Exploration nudge", False),
]

# ---------- helpers ----------
def ensure_out():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

def _norm(s: str) -> str:
    """Lowercase; collapse spaces/underscores/hyphens to a single dash."""
    return re.sub(r"[\s_\-]+", "-", (s or "").lower())

def html_to_text(html: str) -> str:
    if not html:
        return ""
    html = re.sub(r"(?is)<li[^>]*>\s*", "\n- ", html)
    html = re.sub(r"(?is)<br\s*/?>", "\n", html)
    html = re.sub(r"(?is)</(p|div|section|ul|ol|h[1-6])>", "\n\n", html)
    html = re.sub(r"(?is)<[^>]+>", "", html)
    text = htmllib.unescape(html)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()

def compose_markdown(sections: dict) -> str:
    out = {
        "Opener": sections.get("Opener", "").strip(),
        "Context bridge": sections.get("Context bridge", "").strip(),
        "Value moment": sections.get("Value moment", "").strip(),
        "Exploration nudge": sections.get("Exploration nudge", "").strip(),
        "Objections": sections.get("Objections", "").strip(),
        "Next step (salesperson-chosen)": "{{next_step}}",
        "Close": "Thank you for your time.",
    }
    if sections.get("Next step"):
        pre = sections["Next step"].strip()
        if pre:
            out["Next step (salesperson-chosen)"] = pre + "\n\n{{next_step}}"
    if out["Value moment"]:
        out["Value moment"] += "\n\n{{value_proposition}}"
    else:
        out["Value moment"] = "{{value_proposition}}"

    order = ["Opener","Context bridge","Value moment","Exploration nudge","Objections","Next step (salesperson-chosen)","Close"]
    return "\n\n".join(f"# {h}\n{out[h].rstrip()}" for h in order).strip() + "\n"

def write_md(buyer_id: str, md: str):
    ensure_out()
    file_path = OUT_DIR / f"{buyer_id}.md"
    file_path.write_text(md, encoding="utf-8")
    print(f"✓ Wrote {file_path}")

def map_inner_sections_to_canonical(text: str) -> dict:
    lines = [l.rstrip() for l in text.splitlines()]
    buckets, current = {}, "Opener"
    buckets[current] = ""
    def push(line): buckets[current] = (buckets.get(current, "") + ("\n" if buckets.get(current) else "") + line).strip()
    for raw in lines:
        line = raw.strip()
        if not line: continue
        matched = False
        for pattern, target, append in SECTION_RULES:
            if pattern.match(line):
                current = target
                if not append: buckets.setdefault(current, "")
                matched = True
                break
        if not matched: push(line)
    return buckets

# ---------- URL MODE ----------
def run_url_mode(url: str):
    if not HAVE_BS:
        print("URL mode requires BeautifulSoup. Install with: pip install beautifulsoup4 html5lib")
        sys.exit(1)
    print(f"Fetching: {url}")
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req) as resp:
        raw_html = resp.read().decode("utf-8", errors="ignore")
    soup = BeautifulSoup(raw_html, "html5lib")

    # find anchors for each buyer type (by text, id, class)
    anchors = []
    for tag in soup.find_all(True):
        txt = (tag.get_text(" ", strip=True) or "").lower()
        ident = " ".join([tag.get("id",""), " ".join(tag.get("class") or [])]).lower()
        candidate = txt + " " + ident
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
        stop_set = {id(x[1]) for x in anchors_ordered[i+1:]}
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

# ---------- DOCX MODE ----------
def run_docx_mode(pattern: str):
    try:
        import mammoth
    except ImportError:
        print("Please install mammoth for DOCX mode: pip install mammoth")
        sys.exit(1)

    # Accept BOTH a glob and an exact file path
    if os.path.isfile(pattern):
        files = [pattern]
    else:
        files = sorted(glob.glob(pattern))

    if not files:
        print(f"✗ No .docx files matched: {pattern}")
        sys.exit(1)

    for f in files:
        print(f"Reading {f}")
        with open(f, "rb") as fh:
            result = mammoth.convert_to_html(fh)
            html = result.value or ""

        # robust buyer detection: filename OR document body
        nf = _norm(os.path.basename(f))   # e.g. connectivity-early-adopters.docx
        nh = _norm(html)
        buyer = None
        matched_key = None

        # Primary mapping
        for key, val in BUYER_MAP.items():
            nk = _norm(key)               # e.g. early-adopters
            if nk in nf or nk in nh:
                buyer = val
                matched_key = key
                break

        # Fallback tokens if needed
        if not buyer:
            tokens = set(re.split(r"[^a-z0-9]+", nf)) | set(re.split(r"[^a-z0-9]+", nh))
            tokens = {t for t in tokens if t}
            def has(*words): return all(w in tokens for w in words)
            if has("innovators") or has("innovator"):
                buyer, matched_key = "innovator", "innovators*"
            elif has("early","adopters") or has("early","adopter"):
                buyer, matched_key = "early-adopter", "early adopters*"
            elif has("early","majority"):
                buyer, matched_key = "early-majority", "early majority*"
            elif has("late","majority"):
                buyer, matched_key = "late-majority", "late majority*"
            elif any(has(x) for x in [["sceptics"],["sceptic"],["skeptics"],["skeptic"]]):
                buyer, matched_key = "sceptic", "sceptics*"

        print(f"  · detect -> file='{os.path.basename(f)}' nf='{nf}' | match='{matched_key}' => buyer='{buyer}'")

        if not buyer or buyer not in VALID_BUYERS:
            print(f"  ! Skipping (buyer not recognised): {f}")
            continue

        text = html_to_text(html)
        sections = map_inner_sections_to_canonical(text)
        md = compose_markdown(sections)
        write_md(buyer, md)

# ---------- main ----------
def main():
    if not args.url and not args.docx:
        print("Provide either --url or --docx")
        sys.exit(1)
    if args.url:
        run_url_mode(args.url)
    if args.docx:
        run_docx_mode(args.docx)
    print("Done.")

if __name__ == "__main__":
    ensure_out()
    main()
