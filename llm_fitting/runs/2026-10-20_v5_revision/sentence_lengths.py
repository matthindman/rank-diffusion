"""Sentence-length distributions of the PNAS exemplars and of the draft, from reading-order PDF text (pdftotext,
no -layout), cut before acknowledgments/references.  A rough, identical pipeline for all documents.

    python sentence_lengths.py <exemplar_text_dir> <draft.pdf> [more.pdf ...]
"""
import re, subprocess, sys, pathlib, json
import numpy as np

STOP = re.compile(r"\n\s*(ACKNOWLEDGMENTS|Acknowledgments|References|REFERENCES|Data, Materials, and Software Availability|Data Availability|Data, Materials)\b")
def body(txt):
    m = STOP.search(txt)
    txt = txt[:m.start()] if m else txt
    txt = re.sub(r"-\n(?=[a-z])", "", txt)            # de-hyphenate
    txt = re.sub(r"\s+", " ", txt)
    return txt
def sentences(txt):
    s = re.split(r"(?<=[.?!])\s+(?=[A-Z(\[])", txt)
    out = []
    for x in s:
        w = re.findall(r"[A-Za-z][A-Za-z'’\-]*|\d[\d.,]*", x)
        if 3 <= len(w) <= 150 and sum(c.isalpha() for c in x) > 0.5 * len(x.replace(" ", "")):
            out.append(len(w))
    return np.array(out)
def summary(n):
    q = np.percentile(n, [10, 25, 50, 75, 90])
    return dict(n=int(n.size), mean=round(float(n.mean()), 1), p10=q[0], p25=q[1], p50=q[2], p75=q[3], p90=q[4],
                share_lt15=round(float((n < 15).mean()), 3), share_gt30=round(float((n > 30).mean()), 3),
                share_gt40=round(float((n > 40).mean()), 3), sd=round(float(n.std()), 1))
res = {}
for f in sorted(pathlib.Path(sys.argv[1]).glob("*.pages.txt")):
    t = re.sub(r"===== PDF PAGE \d+ of \d+ =====", " ", f.read_text(errors="ignore"))
    res[f.name.split("-et-al")[0].split("-o-dwyer")[0].split("-ugander")[0].split("-pillow")[0].split("-kori")[0]] = summary(sentences(body(t)))
for pdf in sys.argv[2:]:
    t = subprocess.run(["pdftotext", pdf, "-"], capture_output=True, text=True).stdout
    res["DRAFT:" + pathlib.Path(pdf).stem] = summary(sentences(body(t)))
keys = ["n", "mean", "sd", "p10", "p25", "p50", "p75", "p90", "share_lt15", "share_gt30", "share_gt40"]
print("doc".ljust(26) + "".join(k.rjust(10) for k in keys))
for d, r in res.items():
    print(d[:25].ljust(26) + "".join(f"{r[k]:10}" for k in keys))
ex = [r for d, r in res.items() if not d.startswith("DRAFT")]
print("EXEMPLAR MEDIAN".ljust(26) + "".join(f"{np.median([r[k] for r in ex]):10.3g}" for k in keys))
print("EXEMPLAR P25".ljust(26) + "".join(f"{np.percentile([r[k] for r in ex], 25):10.3g}" for k in keys))
print("EXEMPLAR P75".ljust(26) + "".join(f"{np.percentile([r[k] for r in ex], 75):10.3g}" for k in keys))
json.dump(res, open(pathlib.Path(__file__).with_name("sentence_lengths_output.json"), "w"), indent=1, default=float)
