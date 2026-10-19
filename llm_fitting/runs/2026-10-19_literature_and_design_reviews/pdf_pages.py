import subprocess, sys, pathlib, re, zipfile, html
src = pathlib.Path(sys.argv[1]); out = pathlib.Path(sys.argv[2])
names = []
for pdf in sorted(src.glob("*.pdf")):
    stem = pdf.stem
    if not re.search(r"-(et-al|o-dwyer|ugander|pillow|kori)-\d{4}-", stem):
        continue
    txt = subprocess.run(["pdftotext", "-layout", str(pdf), "-"], capture_output=True, text=True).stdout
    pages = txt.split("\f")
    body = []
    for i, p in enumerate(pages, 1):
        if p.strip():
            body.append(f"\n===== PDF PAGE {i} of {len(pages)-1 if not pages[-1].strip() else len(pages)} =====\n{p}")
    clean = re.sub(r"\s*\(\d+\)$", "", stem)
    (out / f"{clean}.pages.txt").write_text(f"SOURCE PDF: {pdf}\n" + "".join(body))
    names.append(clean)
# epub-only papers
have = set(names)
for ep in sorted(src.glob("*.epub")):
    clean = re.sub(r"^\d+-", "", ep.stem)
    if clean in have: continue
    z = zipfile.ZipFile(ep)
    parts = [n for n in z.namelist() if n.endswith((".xhtml", ".html", ".htm"))]
    text = []
    for n in sorted(parts):
        s = z.read(n).decode("utf-8", "ignore")
        s = re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", s, flags=re.S)
        s = re.sub(r"<br\s*/?>|</p>|</h\d>|</li>", "\n", s)
        s = html.unescape(re.sub(r"<[^>]+>", " ", s))
        text.append(re.sub(r"[ \t]+", " ", s))
    (out / f"{clean}.epub.txt").write_text(f"SOURCE EPUB: {ep}\nNOTE: converted from EPUB; no page numbers available. Cite by section heading.\n" + "\n".join(text))
    names.append(clean + " (epub)")
print(len(names)); print("\n".join(names))
