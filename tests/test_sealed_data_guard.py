"""The sealed-data guard: what it blocks, what it lets through.

The guard keeps agents from reading data that can confirm the model only
once (llm_fitting/EXPOSURE_LEDGER.md). Its rule: a command line that
concerns a sealed folder may contain only opaque operations and plain
filters of their output. These tests use a synthetic layout in a temporary
directory, so they never touch the real drives. Many of the cases come from
an independent adversarial review of the first version (MODEL_STATUS 2z-am).
"""
import importlib.util
import json
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
GUARD = REPO / "scripts" / "sealed_data_guard.py"

spec = importlib.util.spec_from_file_location("sealed_data_guard", GUARD)
guard = importlib.util.module_from_spec(spec)
spec.loader.exec_module(guard)


@pytest.fixture()
def lay(tmp_path):
    """A fake repo with a symlinked data drive and two sealed folders."""
    root = Path(os.path.realpath(tmp_path))
    repo = root / "repo"
    drive = root / "drive" / "data root"   # a space, like the real WD path
    for d in (repo / "llm_fitting", repo / "scripts", repo / "data",
              repo / ".claude", repo / "tmp", drive / "wiki" / "raw",
              drive / "bridge" / "reddit", drive / "bridge" / "acquisition",
              drive / "panels", drive / "wiki_other"):
        d.mkdir(parents=True)
    (drive / "wiki" / "raw" / "a.gz").write_text("x")
    (drive / "wiki" / "manifest.json").write_text("{}")
    (drive / "bridge" / "reddit" / "RC_2023-01.zst").write_text("x")
    (drive / "bridge" / "reddit.aria2").write_text("ctl")
    (drive / "bridge" / "acquisition" / "aria2.log").write_text("log")
    (drive / "panels" / "fb.parquet").write_text("x")
    (repo / "data" / "ssd").symlink_to(drive)
    for name in ("pilot.py", "copier.py", "fit.py"):
        (repo / "scripts" / name).write_text("print('hi')\n")
    listing = {
        "sealed": [
            {"id": "bridge", "status": "SEALED",
             "paths": [str(drive / "bridge")],
             "open_subpaths": ["acquisition", "reddit.aria2"]},
            {"id": "wiki", "status": "EXPOSURE UNKNOWN",
             "paths": [str(drive / "wiki"), "data/ssd/wiki"],
             "scripts_reading_it_by_default": ["scripts/pilot.py"]},
        ],
        "approved_runners": ["scripts/copier.py"],
        "released": [],
    }
    lp = repo / "llm_fitting" / "SEALED_PATHS.json"
    lp.write_text(json.dumps(listing))
    ns = type("Lay", (), {})()
    ns.reg = guard.Registry(str(lp), str(repo))
    ns.repo, ns.drive = repo, drive
    ns.B, ns.W = str(drive / "bridge"), str(drive / "wiki")
    return ns


def fill(lay, template):
    return template.format(B=lay.B, W=lay.W, D=str(lay.drive), G=GUARD)


def verdict(lay, cmd, cwd=None):
    return lay.reg.check_command(cmd, str(cwd or lay.repo)).verdict


ALLOWED = [
    # opaque operations inside a sealed folder
    'ls -la "{B}/reddit" 2>&1 | head -12',
    'du -sh "{B}"/*',
    'stat -f %z "{B}/reddit/RC_2023-01.zst"',
    'shasum -a 256 "{B}/reddit/RC_2023-01.zst"',
    'shasum -a 256 < "{B}/reddit/RC_2023-01.zst"',
    'openssl dgst -sha256 "{B}/reddit/RC_2023-01.zst"',
    'zstd -t "{B}/reddit/RC_2023-01.zst"',
    'zstd -t < "{B}/reddit/RC_2023-01.zst"',
    'zstd -tq "{B}"/reddit/*.zst',
    '/usr/bin/time -l zstd -t "{B}/reddit/RC_2023-01.zst"',
    'wc -c "{B}/reddit/RC_2023-01.zst"',
    'wc -c < "{B}/reddit/RC_2023-01.zst"',
    'find "{B}" -name "*.zst" -exec shasum -a 256 {{}} +',
    'find "{B}" -type f | xargs shasum -a 256',
    'find "{B}" -name "*.zst" | xargs nice zstd -t',
    'find "{B}" -name "*.zst" | xargs -n1 -P4 sh -c \'zstd -t "$0" && echo ok\'',
    'find "{B}" -type f | wc -l',
    'find "{B}" -type f | sed "s|{B}/||"',
    'ls -la "{B}/reddit" | awk -F "\\t" \'{{s+=$5}} END{{print s}}\'',
    'D="{B}"; ls "$D/reddit"; shasum "$D/reddit/RC_2023-01.zst"',
    'for f in "{B}"/reddit/*.zst; do zstd -t "$f"; done',
    'cp "{B}/reddit/RC_2023-01.zst" "{B}/reddit/copy.zst"',
    'rsync -an "{B}/" /tmp/elsewhere/',
    'cmp -s "{B}/reddit/RC_2023-01.zst" "{B}/reddit/copy.zst"',
    'diff <(ls "{B}/reddit" | sort) <(sort expected.txt)',
    'comm -13 <(ls "{B}/reddit" | sort) <(sort expected.txt)',
    'aria2c --dir="{B}/reddit" --input-file="{B}/acquisition/list.txt"',
    'curl -L -o "{B}/reddit/x.zst" https://example.org/x.zst',
    'mkdir -p "{B}/reddit/submissions"',
    'ls data/ssd/wiki/raw',
    'ls data/ssd | grep -i wiki',
    'ls data/ssd/*/ | head',
    'du -sh data/ssd/* | sort -h',
    'shasum -a 256 -c "{B}/acquisition/manifest.sha256"',
    'file "{B}/reddit/RC_2023-01.zst"',
    'ls "{B}/reddit" | while read f; do printf "%s %s\\n" "$f" "$(stat -f %z "{B}/reddit/$f")"; done',
    'until [ -f "{B}/reddit/RC_2024-06.zst" ]; do sleep 300; done; echo done',
    'cd "{W}" && ls -la && du -sh raw',
    'test -d "{W}" && echo mounted',
    'OUT="{B}/reddit" ls -la',
    'echo "{B}/reddit" | xargs ls -la',
    # the readable sub-folder and control file
    'tail -n 20 "{B}/acquisition/aria2.log"',
    'grep -c NOTICE "{B}/acquisition/aria2.log"',
    'python3 -c "print(1)" < "{B}/acquisition/aria2.log"',
    'tail -3 "$(ls -t "{B}"/acquisition/*.log | head -1)"',
    'LOG=$(ls -t "{B}"/acquisition/*.log | head -1); tail -3 "$LOG"',
    'cp "{B}/reddit.aria2" "{B}/acquisition/reddit.aria2.bak"',
    # naming the folder in text is not reading it
    'echo "sealed: {B}"',
    'echo "- sealed: {W}"',
    'grep -n "{W}" llm_fitting/SEALED_PATHS.json',
    'grep -n -A 3 "{W}" llm_fitting/SEALED_PATHS.json',
    'rg -n -t md "data/ssd/wiki"',
    'jq \'select(.paths[] == "{W}")\' llm_fitting/SEALED_PATHS.json',
    'sed -i "" "s#data/ssd/wiki#X#" llm_fitting/notes.md',
    'git commit -m "seal {W} until the owner says otherwise"',
    'git commit -m "- seal {W}"',
    'git commit -m "$(cat <<\'EOF\'\nSeals {W} until the owner\'s word (see notes).\n\nCo-Authored-By: X\nEOF\n)"',
    'git commit -F - <<EOF\nseal {W}; don\'t read it\nEOF',
    'git commit -m "$(cat <<\'EOF\'\nSealed-data guard: the owner\'s rule\n\nThe list covers data/ssd/wiki and {B}; nothing is read.\n\nCo-Authored-By: A <a@b.c>\nEOF\n)"',
    'git add -A && git commit -m "$(cat <<\'EOF\'\nseal {W}\nEOF\n)" && git status --short',
    'git grep -n "data/ssd/wiki"',
    'cat > notes.md <<EOF\nThe sealed folder is {W}; it isn\'t to be read.\nEOF',
    'true # {W} mentioned only in a comment',
    # ordinary work elsewhere, including the rest of the data drive
    'python3 scripts/fit.py --ssd-root data/ssd --platform facebook_a',
    'cat data/ssd/panels/fb.parquet | head -c 10',
    'grep -rn sealed llm_fitting scripts',
    'grep -rn sealed .',
    'rg sealed',
    'python3 -m pytest tests/ -q',
    'python3 - <<\'EOF\'\nimport json\nd = {{"a": 1, "b": [1, 2]}}\nprint(json.dumps(d))\nEOF',
    'ls "{D}/wiki_other"',
    'cat "{D}/wiki_other/x"',
    'cp -R "{D}/panels" /tmp/panels',
    'rsync -an "{D}/" /tmp/everything/',
    'for f in *.md; do head -3 "./$f"; done',
    # reading, staging and testing a script that reads a sealed folder
    'cat scripts/pilot.py',
    'sed -n 1,40p scripts/pilot.py',
    'git add scripts/pilot.py',
    'python3 -m pytest tests/test_pilot.py -q',
    # an approved runner, and the guard's own command line
    'python3 scripts/copier.py --src "{B}/reddit" --dst "{B}/copy"',
    'python3 {G} --check-command \'cat "{W}/raw/a.gz"\'',
    'cd "{B}" && python3 {G} --list',
]

BLOCKED = [
    # direct reads
    'cat "{B}/reddit/RC_2023-01.zst"',
    'head -c 100 "{W}/raw/a.gz"',
    'zstd -dc "{B}/reddit/RC_2023-01.zst" | head -5',
    'zstdcat "{B}/reddit/RC_2023-01.zst" | head -5',
    'zstd -d "{B}/reddit/RC_2023-01.zst" -o /tmp/out.ndjson',
    'zstd -q "{B}/reddit/RC_2023-01.zst" --output-dir-flat /tmp/out',
    'gzip -dc "{W}/raw/a.gz"',
    'wc -l "{W}/raw/a.gz"',
    'strings "{B}/reddit/RC_2023-01.zst" | head',
    'grep -c foo "{W}/raw/a.gz"',
    'head -3 < "{W}/raw/a.gz"',
    'file -f "{W}/raw/a.gz"',
    'md5 -p < "{W}/raw/a.gz"',
    'open "{W}/raw/a.gz"',
    'shasum -c "{W}/raw/a.gz"',
    'tree --fromfile "{W}/raw/a.gz"',
    'trap \'cat "{W}/raw/a.gz"\' EXIT',
    'aria2c -i "{W}/raw/a.gz"',
    # the same bytes by other spellings and routes
    'cat data/ssd/wiki/raw/a.gz',
    'cat "/{W}/raw/a.gz"',
    'cat "{D}/panels/../wiki/raw/a.gz"',
    'cat "{D}"/w*/raw/a.gz',
    'cat "{D}"/{{wiki,panels}}/raw/a.gz',
    'cat data/***/manifest.json',
    'cd data/ssd && cat wiki/raw/a.gz',
    'cd "{W}/raw" && head -c 20 a.gz',
    'cd "{W}/raw" && cd /tmp && cd - && head a.gz',
    'cd "{W}/raw" && cd .. && cat "$OLDPWD/a.gz"',
    'test -f "{W}/raw/a.gz" && cat $_',
    'caffeinate -dims nohup cat "{W}/raw/a.gz"',
    # variables, loops, substitutions
    'D="{W}"; cat "$D/raw/a.gz"',
    'ROOT="${{SSD_ROOT:-{D}}}"; head -2 "$ROOT/wiki/manifest.json"',
    'f=$(ls "{B}"/reddit/*.zst | head -1); zstd -dc "$f" | head -2',
    'FILES=$(find "{B}" -name "*.zst"); for f in $FILES; do zstd -dc "$f" | head -1; done',
    'D2="{W}/raw"; gzip -dc "$(ls "$D2"/*.gz | head -1)" | head -3',
    'cd data/ssd && gzip -dc $(ls wiki/raw/*.gz | head -1) | head -3',
    'cd "{D}" && cat $(ls wiki/manifest*.json | head -1)',
    'cd "{D}" && ls | while read d; do head -2 "$d/README.md" 2>/dev/null; done',
    'python3 -c "import glob; print(open(glob.glob(\'data/ssd/*/raw/a.gz\')[0]).read())"',
    'for f in "{B}"/reddit/*.zst; do zstd -dc "$f" | head -1; done',
    'for d in panels wiki; do head -2 "{D}"/$d/README.md; done',
    'for d in $(ls "{D}"); do head -2 "{D}/$d/README.md"; done',
    'ls "{D}" | while read d; do head -2 "{D}/$d/README.md"; done',
    'ls "{D}" | xargs -I{{}} head -2 "{D}/{{}}/README.md"',
    'cat $(find "{B}" -name "*.zst" | head -1)',
    'cat $(echo "{W}/raw/a.gz")',
    'ls "{B}"/reddit/*.zst | head -1 > tmp/first.txt; zstd -dc "$(cat tmp/first.txt)" | head -2',
    # pipes, xargs, find
    'find "{B}" -name "*.zst" | xargs zstdcat | head',
    'find "{B}" -name "*.zst" -exec cat {{}} +',
    'find "{D}" -name "*.gz" | xargs zcat | head',
    'find "{B}" -type f | while read f; do cat "$f"; done',
    'while read f; do cat "$f"; done < <(find "{B}" -type f)',
    'printf "%s\\n" "{B}"/reddit/*.zst | xargs -n1 zstdcat | head',
    'echo "{W}/raw/a.gz" | xargs cat',
    'find "{B}" -type f | awk \'{{while((getline l < $0)>0) print l}}\'',
    'find "{B}" -type f | perl -ne \'chomp; open(F,$_); print <F>\'',
    'find "{B}" -name "*.zst" | python3 -c "import sys,subprocess; [subprocess.run([\'zstd\',\'-dc\',l.strip()]) for l in sys.stdin]"',
    # redirections
    'read line < "{W}/raw/a.gz"; echo "$line"',
    'while read l; do echo "$l"; done < "{W}/raw/a.gz"',
    'exec 3< "{W}/raw/a.gz"; cat <&3',
    'echo "$(< "{W}/raw/a.gz")"',
    # shells inside shells
    'sh -c \'cat "{W}/raw/a.gz"\'',
    'bash -lc \'cd data/ssd && head -2 wiki/manifest.json\'',
    'bash -c \'zstd -dc "$1" | head -2\' _ "{B}/reddit/RC_2023-01.zst"',
    'eval cat "{W}/raw/a.gz"',
    'echo "$(cat "{W}/raw/a.gz")"',
    'git commit -m "$(cat "{W}/raw/a.gz")"',
    'x="$(python3 -c "print(open(\'{W}/raw/a.gz\').read())")"; echo "$x"',
    # recursive searches that would pass through a sealed folder
    'grep -r foo "{D}"',
    'grep -R foo data',
    'grep -rS foo .',
    'grep -d recurse foo "{D}"',
    'rg foo "{D}"',
    'grep -r foo data/ssd',
    # interpreters and unknown programs on a line that names the folder
    'python3 -c "import gzip; print(gzip.open(\'{W}/raw/a.gz\').read())"',
    'python3 -c "open(\'{D}//wiki/x\')"',
    'python3 -c "import pandas as pd; pd.read_parquet(path=\'{W}/hourly/x.parquet\')"',
    'python3 scripts/fit.py --input "{B}/reddit/RC_2023-01.zst"',
    'python3 scripts/fit.py --input="{B}/reddit/RC_2023-01.zst"',
    'ls "{W}" >/dev/null && python3 scripts/fit.py -i"{W}/raw/a.gz"',
    'DATA_DIR="{W}" python3 scripts/fit.py',
    'env DATA_DIR="{W}" python3 scripts/fit.py',
    'export WIKI="{W}"; python3 -c "import os; print(open(os.environ[\'WIKI\']+\'/x\').read())"',
    'duckdb -c "select * from \'{W}/raw/a.gz\' limit 5"',
    'cd "{D}" && duckdb -c "select * from \'wiki/raw/*.gz\' limit 5"',
    'python3 - <<EOF\nimport gzip\nprint(gzip.open("{W}/raw/a.gz").read())\nEOF',
    'python3 - <<\'EOF\'\nfile = "{W}/raw/a.gz"\nprint(open(file).read())\nEOF',
    'python3 - <<\'EOF\'\np="{W}/raw/a.gz"\nprint(open(p).read())\nEOF',
    'ls "{W}" && python3 <<< "print(open(\'{W}/manifest.json\').read())"',
    'docker run -v "{W}":/data img cat /data/raw/a.gz',
    'awk \'BEGIN{{while((getline l < "{W}/raw/a.gz")>0) print l}}\'',
    # the folder reached from its parent plus its own name
    'python3 -c "from pathlib import Path; root = Path(\'{D}\'); print(list((root / \'wiki\').rglob(\'*\')))"',
    'python3 -c "import os; print(os.listdir(os.path.join(\'data/ssd\',\'wiki\',\'raw\')))"',
    'python3 scripts/fit.py --ssd-root data/ssd --platform wiki',
    # a script whose defaults point into the folder, by any route
    'python3 scripts/pilot.py aggregate',
    'caffeinate -dims python3 -u scripts/pilot.py aggregate',
    '/usr/bin/time -l python3 -u scripts/pilot.py aggregate',
    'python3 -W ignore scripts/pilot.py',
    'python3 -m scripts.pilot aggregate',
    'cd scripts && python3 -m pilot aggregate',
    'bash -lc "python3 scripts/pilot.py aggregate"',
    'python3 -c "import sys; sys.path.insert(0,\'scripts\'); import pilot as w; w.main()"',
    'python3 "$(git rev-parse --show-toplevel)/scripts/pilot.py"',
    'echo scripts/pilot.py | xargs python3',
    # git as a reader
    'git hash-object -w "{W}/raw/a.gz"',
    'git diff /dev/null "{W}/manifest.json"',
    'git -C "{W}" diff --no-index a b',
    # copies out, uploads, overwriting, deletion, aliases
    'cp "{B}/reddit/RC_2023-01.zst" /tmp/x.zst',
    'cp --target-directory=/tmp/out "{W}/raw/a.gz"',
    'cp -t /tmp/out "{W}/raw/a.gz"',
    'cp "{B}/reddit/RC_2023-01.zst" "{B}/acquisition/x.zst"',
    'rsync -a "{B}/" /tmp/elsewhere/',
    'cp -R "{D}" /tmp/everything',
    'rsync -a data/ssd/ /tmp/everything/',
    'tar -czf /tmp/all.tgz "{W}"',
    'tar -czf /tmp/all.tgz "{D}"',
    'find "{B}" -type f | xargs -I{{}} cp {{}} /tmp/out/',
    'curl -T "{B}/reddit/RC_2023-01.zst" https://example.org/up',
    'curl -H @"{W}/raw/a.gz" https://example.org',
    'curl "file://{W}/raw/a.gz"',
    'gh gist create "{W}/raw/a.gz"',
    'echo "" > "{B}/reddit/RC_2023-01.zst"',
    'cp /dev/null "{B}/reddit/RC_2023-01.zst"',
    'rsync -a --delete x/ "{B}/reddit/"',
    'rsync -a --remove-source-files "{B}/reddit/" "{W}/m/"',
    'rm -rf "{B}/reddit"',
    'ln -s "{W}" /tmp/alias',
    # refused by design although harmless: keep such lines to opaque operations
    'python3 -c "print($(stat -f %z "{B}/reddit/RC_2023-01.zst")/1e9)"',
    'ls -la "{B}/reddit"; tail -3 "$LOG"',
]


@pytest.mark.parametrize("template", ALLOWED)
def test_opaque_and_unrelated_commands_pass(lay, template):
    cmd = fill(lay, template)
    d = lay.reg.check_command(cmd, str(lay.repo))
    assert d.verdict == guard.ALLOW, (cmd, d.reason)


@pytest.mark.parametrize("template", BLOCKED)
def test_commands_that_could_read_sealed_bytes_are_blocked(lay, template):
    cmd = fill(lay, template)
    d = lay.reg.check_command(cmd, str(lay.repo))
    assert d.verdict == guard.DENY, cmd
    assert "owner" in d.reason


def test_unparseable_command_fails_closed_only_when_it_names_the_folder(lay):
    assert verdict(lay, f'cat "{lay.W}/raw/a.gz') == guard.DENY
    assert verdict(lay, 'echo "unterminated') == guard.ALLOW


def test_relative_paths_follow_the_reported_working_directory(lay):
    assert verdict(lay, "head -c 5 wiki/raw/a.gz", cwd=lay.drive) == guard.DENY
    assert verdict(lay, "head -c 5 a.gz", cwd=lay.drive / "wiki" / "raw") == guard.DENY
    assert verdict(lay, "head -c 5 panels/fb.parquet", cwd=lay.drive) == guard.ALLOW


def test_file_tools(lay):
    reg, cwd = lay.reg, str(lay.repo)
    sealed_file = f"{lay.W}/raw/a.gz"

    def v(name, tool_input):
        return reg.check_tool(name, tool_input, cwd).verdict

    assert v("Read", {"file_path": sealed_file}) == guard.DENY
    assert v("Read", {"file_path": "data/ssd/wiki/raw/a.gz"}) == guard.DENY
    assert v("Read", {"file_path": f"{lay.B}/acquisition/aria2.log"}) == guard.ALLOW
    assert v("Read", {"file_path": f"{lay.drive}/panels/fb.parquet"}) == guard.ALLOW
    assert v("Grep", {"pattern": "x", "path": lay.W}) == guard.DENY
    assert v("Grep", {"pattern": "x", "path": str(lay.drive)}) == guard.DENY
    assert v("Grep", {"pattern": "x"}) == guard.ALLOW
    assert v("Glob", {"pattern": f"{lay.W}/**"}) == guard.ALLOW
    assert v("Write", {"file_path": sealed_file, "content": ""}) == guard.DENY
    # text that only mentions the folder is not an access
    assert v("Write", {"file_path": "notes.md", "content": sealed_file}) == guard.ALLOW
    assert v("Agent", {"prompt": f"do not read {sealed_file}"}) == guard.ALLOW
    assert v("SendMessage", {"to": "x", "message": f"{lay.W}/ is sealed"}) == guard.ALLOW
    # other tools: commands, path fields and addresses are checked
    assert v("mcp__terminal__run_in_terminal", {"command": f'cat "{sealed_file}"'}) == guard.DENY
    assert v("Monitor", {"command": f'tail -f "{sealed_file}"'}) == guard.DENY
    assert v("Monitor", {"command": f'tail -f "{lay.B}/acquisition/aria2.log"',
                         "description": f"download into {lay.B}/reddit"}) == guard.ALLOW
    assert v("SendUserFile", {"files": [sealed_file]}) == guard.DENY
    assert v("SomeNewTool", {"source": sealed_file}) == guard.DENY
    assert v("SomeNewTool", {"source": "data/ssd/wiki/manifest.json"}) == guard.DENY
    assert v("SomeNewTool", {"mode": "fast", "source": "data/ssd/panels/fb.parquet"}) == guard.ALLOW
    url = "file://" + sealed_file.replace(" ", "%20")
    assert v("mcp__Claude_Browser__navigate", {"url": url}) == guard.DENY
    assert v("mcp__Claude_Browser__navigate", {"url": "file:" + sealed_file}) == guard.DENY
    assert v("mcp__Claude_Browser__navigate", {"url": "https://example.org/wiki"}) == guard.ALLOW
    assert v("mcp__ide__executeCode", {"code": f"print(open('{sealed_file}').read())"}) == guard.DENY
    assert v("mcp__ide__executeCode", {"code": "print(1)"}) == guard.ALLOW
    assert v("mcp__jupyter__run_cell", {"source": f"open('{lay.drive}/wiki/raw/a.gz')"}) == guard.DENY
    assert v("WebFetch", {"url": "file://" + sealed_file, "prompt": "x"}) == guard.DENY
    assert v("WebFetch", {"url": "https://en.wikipedia.org/wiki/Zipf", "prompt": "x"}) == guard.ALLOW
    assert v("Grep", {"pattern": "x", "glob": "data/ssd/wiki/**"}) == guard.DENY


def test_changing_the_rule_asks_the_owner(lay):
    reg, cwd = lay.reg, str(lay.repo)
    for rel in guard.CONTROL_FILES:
        d = reg.check_tool("Edit", {"file_path": str(lay.repo / rel)}, cwd)
        assert d.verdict == guard.ASK, rel
    for cmd in ("echo '{}' > llm_fitting/SEALED_PATHS.json",
                "sed -i '' s/a/b/ llm_fitting/SEALED_PATHS.json",
                "rm scripts/sealed_data_guard.py",
                "mv llm_fitting/SEALED_PATHS.json x",
                "git checkout -- llm_fitting/SEALED_PATHS.json",
                "python3 -c \"import json; json.dump({}, open('llm_fitting/SEALED_PATHS.json','w'))\"",
                "cp /tmp/new.json .claude/settings.json"):
        assert verdict(lay, cmd) == guard.ASK, cmd
    for cmd in ("cat llm_fitting/SEALED_PATHS.json",
                "git add llm_fitting/SEALED_PATHS.json scripts/sealed_data_guard.py",
                "git diff -- .claude/settings.json",
                "cp .claude/settings.json /tmp/copy.json",
                "python3 -m pytest tests/test_sealed_data_guard.py -q",
                f"python3 {GUARD} --self-check"):
        assert verdict(lay, cmd) == guard.ALLOW, cmd


def test_handing_a_sealed_path_to_another_agent_asks_the_owner(lay):
    assert verdict(lay, f'codex exec "summarise {lay.W}/raw/a.gz"') == guard.ASK
    assert verdict(lay, f'node plugins/codex-companion.mjs task "do not read {lay.W}"') == guard.ASK
    assert verdict(lay, 'codex exec "run the test suite"') == guard.ALLOW


def test_released_entries_are_no_longer_guarded(lay):
    lp = Path(lay.reg.list_path)
    data = json.loads(lp.read_text())
    data["released"] = [e for e in data["sealed"] if e["id"] == "wiki"]
    data["sealed"] = [e for e in data["sealed"] if e["id"] != "wiki"]
    lp.write_text(json.dumps(data))
    reg = guard.Registry(str(lp), str(lay.repo))
    assert reg.check_command(f'cat "{lay.W}/raw/a.gz"', str(lay.repo)).verdict == guard.ALLOW
    assert reg.check_command(f'cat "{lay.B}/reddit/RC_2023-01.zst"', str(lay.repo)).verdict == guard.DENY


def test_require_released(lay):
    with pytest.raises(guard.SealedDataError):
        guard.require_released(f"{lay.W}/raw/a.gz", lay.reg.list_path)
    guard.require_released(f"{lay.drive}/panels/fb.parquet", lay.reg.list_path)


def run_hook(payload, list_file):
    text = payload if isinstance(payload, str) else json.dumps(payload)
    out = subprocess.run([sys.executable, str(GUARD), "--hook", "--list-file", str(list_file)],
                         input=text, capture_output=True, text=True)
    return out.returncode, out.stdout


def test_hook_protocol(lay):
    """The hook prints a deny decision as JSON and is silent otherwise."""
    rc, out = run_hook({"tool_name": "Bash", "cwd": str(lay.repo),
                        "tool_input": {"command": f'cat "{lay.B}/reddit/RC_2023-01.zst"'}},
                       lay.reg.list_path)
    decision = json.loads(out)["hookSpecificOutput"]
    assert rc == 0 and decision["permissionDecision"] == "deny"
    assert decision["hookEventName"] == "PreToolUse"
    assert run_hook({"tool_name": "Bash", "cwd": str(lay.repo),
                     "tool_input": {"command": f'ls "{lay.B}"'}}, lay.reg.list_path) == (0, "")


def test_hook_never_fails_open(lay, tmp_path):
    """A guard that cannot decide refuses anything that names a data drive."""
    drive_call = {"tool_name": "Bash", "cwd": "/",
                  "tool_input": {"command": "cat /Volumes/T9/x"}}
    plain_call = {"tool_name": "Bash", "cwd": "/", "tool_input": {"command": "git status"}}
    bad = tmp_path / "bad.json"
    for content in (None, "not json", "[]", '{"sealed": [{"id": "x"}]}'):
        if content is None:
            bad.unlink(missing_ok=True)
        else:
            bad.write_text(content)
        rc, out = run_hook(drive_call, bad)
        assert rc == 0 and json.loads(out)["hookSpecificOutput"]["permissionDecision"] == "deny"
        assert run_hook(plain_call, bad) == (0, "")
    # odd payloads against a good list
    for payload in ("[1, 2]", '{"tool_name": "Bash", "cwd": 5, "tool_input": {"command": "true"}}',
                    json.dumps({"tool_name": "Bash", "cwd": "/",
                                "tool_input": {"command": "python3 \x00\udc80x.py"}})):
        rc, out = run_hook(payload, lay.reg.list_path)
        assert rc == 0
        assert out == "" or json.loads(out)


def test_guard_is_fast_on_code_with_many_braces(lay):
    literal = ", ".join('{"k%d": %d, "v": [1, 2]}' % (i, i) for i in range(40))
    start = time.time()
    assert verdict(lay, f"python3 -c 'x = [{literal}]; print(len(x))'") == guard.ALLOW
    assert time.time() - start < 1.0


def test_repository_list_is_well_formed():
    data = json.loads((REPO / "llm_fitting" / "SEALED_PATHS.json").read_text())
    ids = [e["id"] for e in data["sealed"] + data["released"]]
    assert len(ids) == len(set(ids))
    for e in data["sealed"]:
        assert e["status"] in ("SEALED", "EXPOSURE UNKNOWN")
        assert e["paths"], e["id"]
        for s in e.get("scripts_reading_it_by_default", []):
            assert (REPO / s).exists(), s
    for s in data.get("approved_runners", []):
        assert (REPO / s).exists(), s
    guard.Registry()  # loads without touching the drives
