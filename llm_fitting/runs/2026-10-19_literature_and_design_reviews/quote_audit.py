"""Extract quotations from a report and check them against local page-marked texts.
Usage: quote_audit.py REPORT.md TEXTDIR [OUT.tsv]
A quotation is any run of 6+ words inside straight or curly double quotes.
Matching: lowercase, unify quotes/dashes, drop hyphenation at line breaks, collapse whitespace;
search every text file; report file and PDF page of the first match."""
import re, sys, pathlib, unicodedata
rep = pathlib.Path(sys.argv[1]).read_text()
tdir = pathlib.Path(sys.argv[2])
def norm(s):
    s = unicodedata.normalize("NFKC", s)
    s = s.replace("’", "'").replace("‘", "'").replace("“", '"').replace("”", '"')
    s = re.sub(r"[‐-―−]", "-", s)
    s = re.sub(r"-\s*\n\s*", "", s)          # hyphenation across lines
    s = re.sub(r"\s+", " ", s).lower()
    s = re.sub(r"\s*\[\d+(?:[,–-]\s*\d+)*\]", "", s)   # bracket citations
    return s.strip()
def strip_punct(s):
    return re.sub(r"[^a-z0-9 ]", "", s)
texts = {}
for f in sorted(tdir.glob("*.txt")):
    raw = f.read_text(errors="ignore")
    pages = re.split(r"===== PDF PAGE (\d+) of \d+ =====", raw)
    segs = []
    if len(pages) > 1:
        for i in range(1, len(pages), 2):
            segs.append((pages[i], pages[i+1]))
    else:
        segs.append(("?", raw))
    full = " ".join(norm(t) for _, t in segs)
    texts[f.name] = (segs, strip_punct(full))
quotes = re.findall(r'["“]([^"“”]{30,600})["”]', rep)
quotes = [q for q in quotes if len(q.split()) >= 6]
out = []
for q in quotes:
    nq = strip_punct(norm(q))
    nq_core = " ".join(nq.split()[:12])  # first 12 words, robust to ellipses later
    parts = [p.strip() for p in re.split(r"\.\.\.|…|\[\.\.\.\]", q) if len(p.split()) >= 4]
    hit = None
    for name, (segs, full) in texts.items():
        if all(strip_punct(norm(p)) in full for p in parts or [q]):
            # find page of first part
            first = strip_punct(norm((parts or [q])[0]))
            pg = "?"
            for p_no, t in segs:
                if first[:60] in strip_punct(norm(t)):
                    pg = p_no; break
            hit = (name, pg); break
    out.append((("FOUND" if hit else "NOT-LOCAL"), hit[0] if hit else "", hit[1] if hit else "", q[:160].replace("\t"," ").replace("\n"," ")))
w = sys.stdout if len(sys.argv) < 4 else open(sys.argv[3], "w")
for r in out: print("\t".join(r), file=w)
n = sum(r[0] == "FOUND" for r in out)
print(f"# {len(out)} quotations; {n} found in local texts", file=sys.stderr)
