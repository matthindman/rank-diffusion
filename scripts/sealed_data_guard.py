#!/usr/bin/env python3
"""Sealed-data guard for the rank-diffusion program.

A dataset can confirm the model only once, and only if nothing was learned
from it beforehand (llm_fitting/EXPOSURE_LEDGER.md). The folders that hold
such data are listed in llm_fitting/SEALED_PATHS.json. This script decides
whether a shell command or a tool call may go ahead.

The rule it applies to a command line:

  1. Does the line concern a sealed folder? It does if it names one, names
     the folder's parent together with the folder's own name, resolves into
     one through a variable, a loop, a glob, a symlink or `cd`, lists a
     folder that contains one recursively, or names a script whose default
     paths point into one.
  2. If so, every simple command on that line must be an opaque operation
     (listing, sizes, checksums, integrity tests, downloads, copies to a
     destination that is itself listed) or a plain filter of the output of
     one. Anything else on the line is refused, including anything this
     script cannot analyse. Unrelated work goes on a separate command.

Obviously destructive commands in a sealed folder (rm, overwriting by
redirection or by copy) are refused as well, but this is not write
protection.

Only the owner unseals. After the owner says so in the conversation, the
entry is moved from "sealed" to "released" in the list.

Command line:
  sealed_data_guard.py --check-command 'zstd -dc /path/x.zst | head'
  sealed_data_guard.py --check-read /path/to/file
  sealed_data_guard.py --list
  sealed_data_guard.py --self-check
  sealed_data_guard.py --hook      Claude Code PreToolUse hook, JSON on stdin

From Python (new builders and intake gates call this on their inputs):
  from sealed_data_guard import require_released
  require_released(path)           raises SealedDataError while still sealed

This is a guard against mistakes, not a lock. It reads the text of one
command. It cannot see what a program opens once it is running, it does not
follow names written to a file by one command and read back by the next,
and it covers only the agents that call it.
"""
from __future__ import annotations

import argparse
import glob as _glob
import itertools
import json
import os
import re
import shlex
import sys
import urllib.parse

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_LIST = os.path.join(REPO, "llm_fitting", "SEALED_PATHS.json")

# Files that define the rule. Changing them asks the owner first.
CONTROL_FILES = (
    "llm_fitting/SEALED_PATHS.json",
    "scripts/sealed_data_guard.py",
    ".claude/settings.json",
    ".claude/settings.local.json",
)

ALLOW, ASK, DENY = "allow", "ask", "deny"
NONE, ANC_LINK, ANC_REAL, OPEN, SEALED = 0, 1, 2, 3, 4

UNK = ""   # a value this script cannot compute
PSUB = ""  # the output stream of a process substitution
_SUB_L, _SUB_R = "", ""
_HD_L, _HD_R = "", ""

LIST = {"ls", "gls", "eza", "exa", "ll", "stat", "gstat", "du", "gdu", "df",
        "tree", "file", "realpath", "grealpath", "readlink", "greadlink",
        "basename", "dirname", "xattr", "lsof", "diskutil", "mount",
        "uptime", "hostname", "whoami", "id", "uname", "sw_vers", "iostat",
        "vm_stat", "pmset"}
HASH = {"shasum", "sha1sum", "sha224sum", "sha256sum", "sha384sum",
        "sha512sum", "gsha256sum", "sha256", "sha1", "md5", "md5sum",
        "gmd5sum", "cksum", "sum", "b2sum", "b3sum", "crc32", "xxhsum",
        "xxh32sum", "xxh64sum", "xxh128sum"}
NOOP = {"echo", "printf", "true", "false", ":", "pwd", "export", "unset",
        "set", "sleep", "wait", "date", "exit", "return", "read", "shift",
        "local", "declare", "typeset", "readonly", "break", "continue",
        "umask", "type", "which", "whereis", "hash", "for", "case",
        "dirs", "alias", "unalias", "pgrep", "ps", "kill", "pkill",
        "mapfile", "readarray", "seq", "expr", "bc", "numfmt"}
TEXT = {"gh", "claude", "logger"}
# Other agents' command lines: this guard does not cover what they do next.
OTHER_AGENTS = {"codex", "gemini", "aider", "llm", "ollama", "opencode",
                "cursor-agent"}
TEST = {"test", "[", "[["}
CD = {"cd", "pushd", "popd"}
ZSTD = {"zstd", "pzstd", "zstdmt"}
OTHERZ = {"gzip", "bzip2", "xz", "lz4", "pigz", "pbzip2", "lzip", "lzma",
          "pixz"}
TAR = {"tar", "bsdtar", "gtar"}
TRANSFER = {"cp", "gcp", "rsync", "ditto", "mv", "gmv"}
DOWNLOAD = {"aria2c", "curl", "wget"}
METAW = {"mkdir", "gmkdir", "touch", "gtouch", "chmod", "chown", "chgrp",
         "chflags"}
GREP = {"grep", "ggrep", "egrep", "fgrep", "rg", "ag", "ack"}
SCRIPTED = {"sed", "gsed", "awk", "gawk", "mawk", "jq"}
FILTER = {"head", "ghead", "tail", "gtail", "sort", "gsort", "uniq", "cut",
          "gcut", "tr", "column", "tee", "cat", "gcat", "nl", "paste", "fold",
          "fmt", "rev", "less", "more", "tac", "comm", "join", "strings",
          "xxd", "hexdump", "od", "base64", "pr"} | GREP | SCRIPTED
MODAL = ZSTD | OTHERZ | TAR | {"unzip", "zipinfo", "wc", "cmp", "diff"}
SHELLS = {"sh", "bash", "zsh", "dash", "ksh"}
XARGS = {"xargs", "gxargs", "parallel"}
FIND = {"find", "gfind", "fd", "fdfind"}
DESTRUCTIVE = {"rm", "grm", "rmdir", "unlink", "shred", "truncate", "dd",
               "ln", "gln", "trash"}
KEYWORDS = {"if", "then", "else", "elif", "fi", "while", "until", "do",
            "done", "{", "}", "!", "esac", "function", "select", "in", "]]"}
WRAPPERS = {  # wrapper -> options that take a value
    "sudo": {"-u", "-g", "-h", "-p", "-C", "-D", "-R", "-T"},
    "env": {"-u", "-C", "-S"},
    "nohup": set(), "command": set(), "builtin": set(), "exec": set(),
    "noglob": set(), "nocorrect": set(), "nice": {"-n"}, "ionice": {"-c", "-n"},
    "caffeinate": {"-t", "-w"}, "stdbuf": set(), "arch": set(),
    "time": {"-o", "-f"}, "gtime": {"-o", "-f"},
    "timeout": {"-s", "-k"}, "gtimeout": {"-s", "-k"},
}
VALUE_OPTS = {  # options whose next word is a value, not a file
    "head": {"-n": 1, "-c": 1}, "tail": {"-n": 1, "-c": 1, "-s": 1},
    "sort": {"-k": 1, "-t": 1, "-S": 1, "-T": 1},
    "cut": {"-d": 1, "-f": 1, "-c": 1, "-b": 1},
    "uniq": {"-f": 1, "-s": 1, "-w": 1},
    "column": {"-s": 1, "-c": 1, "-o": 1}, "paste": {"-d": 1},
    "fold": {"-w": 1}, "nl": {"-b": 1, "-w": 1, "-s": 1, "-v": 1, "-i": 1},
    "xxd": {"-l": 1, "-s": 1, "-c": 1, "-g": 1},
    "od": {"-N": 1, "-j": 1, "-A": 1, "-t": 1, "-w": 1},
    "strings": {"-n": 1}, "join": {"-t": 1, "-1": 1, "-2": 1, "-j": 1},
    "grep": {"-e": 1, "-A": 1, "-B": 1, "-C": 1, "-m": 1, "-d": 1, "-D": 1,
             "--include": 1, "--exclude": 1, "--exclude-dir": 1,
             "--regexp": 1, "--context": 1, "--max-count": 1},
    "rg": {"-e": 1, "-A": 1, "-B": 1, "-C": 1, "-m": 1, "-g": 1, "-t": 1,
           "-T": 1, "-j": 1, "-M": 1, "-d": 1, "-E": 1, "-r": 1,
           "--type": 1, "--type-not": 1, "--glob": 1, "--max-count": 1,
           "--regexp": 1, "--max-depth": 1, "--sort": 1, "--sortr": 1,
           "--encoding": 1, "--replace": 1, "--color": 1},
    "awk": {"-F": 1, "-v": 1}, "sed": {"-e": 1},
    "jq": {"--arg": 2, "--argjson": 2, "--slurpfile": 1, "--rawfile": 1,
           "--indent": 1, "--tab": 0},
    "rsync": {"-e": 1, "--exclude": 1, "--include": 1, "--files-from": 1,
              "--log-file": 1, "--rsync-path": 1, "--bwlimit": 1,
              "--timeout": 1, "--partial-dir": 1, "--temp-dir": 1,
              "--compare-dest": 1, "--link-dest": 1, "-f": 1, "--filter": 1},
    "git": {"-C": 1, "-c": 1},
}
for _alias, _base in (("ghead", "head"), ("gtail", "tail"), ("gsort", "sort"),
                      ("gcut", "cut"), ("ggrep", "grep"), ("egrep", "grep"),
                      ("fgrep", "grep"), ("ag", "rg"), ("ack", "rg"),
                      ("gawk", "awk"), ("mawk", "awk"), ("gsed", "sed")):
    VALUE_OPTS[_alias] = VALUE_OPTS[_base]
OUT_OPTS = {  # options whose value is where a download is written or logged
    "curl": {"-o", "--output", "--output-dir"},
    "wget": {"-O", "--output-document", "-P", "--directory-prefix", "-o",
             "--output-file", "-a", "--append-output"},
    "aria2c": {"-d", "--dir", "-o", "--out", "-l", "--log", "--save-session",
               "--dht-file-path", "--dht-file-path6", "--save-cookies",
               "--server-stat-of"},
}
TEXT_KEYS = {"description", "title", "summary", "reason", "label", "prompt",
             "message", "note", "caption", "name", "content", "text", "body",
             "query", "pattern", "old_string", "new_string", "tldr"}
PATH_KEYS = {"path", "paths", "file", "files", "file_path", "file_paths",
             "filename", "notebook_path", "root", "directory", "dir",
             "out_dir", "from"}

_OPS = set("();<>|&")
_VAR_RE = re.compile(
    r"\$\{([A-Za-z_][A-Za-z0-9_]*)(:?[-=]([^}]*))?[^}]*\}"
    r"|\$([A-Za-z_][A-Za-z0-9_]*)|\$([0-9@*_])")
_ASSIGN_RE = re.compile(r"^([A-Za-z_][A-Za-z0-9_]*)\+?=(.*)$", re.S)
_SUB_RES = ((re.compile(r"\$\(([^()]*)\)"), "cmd"),
            (re.compile(r"`([^`]*)`"), "cmd"),
            (re.compile(r"[<>]\(([^()]*)\)"), "proc"))
_SUB_TOKEN = re.compile(_SUB_L + r"(\d+)" + _SUB_R)
_HD_TOKEN = re.compile(_HD_L + r"(\d+)" + _HD_R)
_NUM_RE = re.compile(r"^[+-]?\d+(\.\d+)?[A-Za-z%]?$")
_SAFE_ENV = ("HOME", "USER", "TMPDIR", "LOGNAME")
_SED_SAFE = re.compile(
    r"^\s*(\d+|\$|/[^/]*/)?(,\s*(\d+|\$|/[^/]*/))?\s*([pdq=]|[sy](.).*)\s*$",
    re.S)

STRICT_HINT = ("On a command line that concerns a sealed folder, only opaque "
               "operations and plain filters of their output are allowed; "
               "run anything else as a separate command.")
RULE_CHANGE = ("This changes a file that defines the sealed-data rule (the "
               "sealed list, the guard, or the hook settings). Only the "
               "owner unseals data or changes the rule; approve only if that "
               "is what you intend.")


class SealedDataError(RuntimeError):
    """Raised by require_released() for a path that is still sealed."""


def _key(path: str) -> str:
    p = os.path.normpath(path)
    if p.startswith("//"):
        p = "/" + p.lstrip("/")
    if p.startswith("/System/Volumes/Data/"):  # macOS: the same files
        p = p[len("/System/Volumes/Data"):]
    return p.casefold()


def _under(child: str, parent: str) -> bool:
    if parent == os.sep:
        return True
    return child == parent or child.startswith(parent + os.sep)


def _resolve_first_link(path: str) -> str:
    """Follow the first symlink on the way to `path`, then stop.

    The sealed roots are resolved on every call, so this must never wait on
    an external drive: nothing under /Volumes is stat'ed here.
    """
    parts = os.path.normpath(path).split(os.sep)
    cur = os.sep
    for i, part in enumerate(parts[1:], 1):
        cur = os.path.join(cur, part)
        if cur.startswith("/Volumes/"):
            break
        try:
            if os.path.islink(cur):
                target = os.readlink(cur)
                if not os.path.isabs(target):
                    target = os.path.join(os.path.dirname(cur), target)
                return os.path.normpath(os.path.join(target, *parts[i + 1:]))
        except OSError:
            break
    return os.path.normpath(path)


def _norm_text(text: str) -> str:
    """Free text as compared against markers: case, quotes and slashes."""
    t = text.casefold().replace("\\ ", " ").replace('"', "").replace("'", "")
    return re.sub(r"/{2,}", "/", t)


def _has_marker(t: str, marker: str, open_names=()) -> bool:
    start = 0
    while True:
        i = t.find(marker, start)
        if i < 0:
            return False
        start = i + 1
        rest = t[i + len(marker):]
        nxt = rest[:1]
        if nxt.isalnum() or nxt in ("_", "-") or (
                nxt == "." and rest[1:2].isalnum()):
            continue  # a longer, different name
        for o in open_names:
            if rest.startswith("/" + o):
                tail = rest[len(o) + 1:len(o) + 2]
                if not (tail.isalnum() or tail in ("_", "-", ".")):
                    break
        else:
            return True


def _has_word(t: str, word: str, open_names=()) -> bool:
    """`word` as a path component or bare word, other than word/open_name."""
    for m in re.finditer(r"(?<![a-z0-9_\-.])" + re.escape(word)
                         + r"(?![a-z0-9_\-]|\.[a-z0-9])", t):
        rest = t[m.end():]
        for o in open_names:
            if rest.startswith("/" + o):
                tail = rest[len(o) + 1:len(o) + 2]
                if not (tail.isalnum() or tail in ("_", "-", ".")):
                    break
        else:
            return True
    return False


def _short(raw) -> set:
    out = set()
    for r in raw:
        if len(r) > 1 and r[0] == "-" and r[1] != "-" and "=" not in r:
            out.update(r[1:])
    return out


def _long(raw) -> set:
    return {r.split("=", 1)[0] for r in raw if r.startswith("--") and len(r) > 2}


def _strings(val):
    if isinstance(val, str):
        yield val
    elif isinstance(val, dict):
        for v in val.values():
            yield from _strings(v)
    elif isinstance(val, (list, tuple)):
        for v in val:
            yield from _strings(v)


class Entry:
    def __init__(self, raw: dict, repo: str):
        self.id = raw["id"]
        self.status = raw.get("status", "SEALED")
        self.what = raw.get("what", "")
        self.declared = list(raw["paths"])
        self.roots, self.real_roots, self.markers = set(), set(), set()
        for p in self.declared:
            a = p if os.path.isabs(p) else os.path.join(repo, p)
            lex, real = _key(a), _key(_resolve_first_link(a))
            self.roots.update((lex, real))
            self.real_roots.add(real)
            self.markers.update((lex, real))
            if not os.path.isabs(p):
                self.markers.add(os.path.normpath(p).casefold())
        self.open_names = [os.path.normpath(s).casefold()
                           for s in raw.get("open_subpaths", [])]
        self.opens = {r + os.sep + s for r in self.roots
                      for s in self.open_names}
        self.parents = {os.path.dirname(r) for r in self.roots}
        self.parent_markers = {os.path.dirname(m) for m in self.markers
                               if len(os.path.dirname(m)) >= 6}
        self.leaves = {os.path.basename(m) for m in self.markers}
        scripts = raw.get("scripts_reading_it_by_default", [])
        self.stems = {os.path.splitext(os.path.basename(s))[0].casefold()
                      for s in scripts}

    def label(self) -> str:
        return f"{self.declared[0]} ({self.status}; {self.id})"


class Decision:
    def __init__(self, verdict=ALLOW, reason="", entry=None):
        self.verdict, self.reason, self.entry = verdict, reason, entry


class Arg:
    __slots__ = ("text", "texts", "raw", "cls", "entry", "unknown", "psub",
                 "redir", "flag", "numeric")

    def __init__(self, text="", redir=None):
        self.text, self.texts, self.redir = text, [text], redir
        self.raw = text  # the word as written, before variables are filled in
        self.cls, self.entry = NONE, None
        self.unknown = self.psub = False
        self.flag = len(text) > 1 and text.startswith("-")
        self.numeric = bool(_NUM_RE.match(text))

    @property
    def safe(self) -> bool:
        """Known not to be a file in a sealed folder."""
        return self.cls != SEALED and not self.unknown


class Registry:
    def __init__(self, list_path: str = DEFAULT_LIST, repo: str = REPO):
        self.repo, self.list_path = repo, list_path
        with open(list_path, encoding="utf-8") as fh:
            data = json.load(fh)
        self.entries = [Entry(raw, repo) for raw in data.get("sealed", [])]
        self.released = data.get("released", [])
        self.approved = {
            _key(os.path.realpath(os.path.join(repo, s)))
            for s in data.get("approved_runners", [])}
        self.control = {_key(os.path.realpath(os.path.join(repo, c)))
                        for c in CONTROL_FILES}
        self.control_names = {os.path.basename(c).casefold()
                              if "settings" not in c else c.casefold()
                              for c in CONTROL_FILES}
        self.self_path = _key(os.path.realpath(__file__))

    # ---- paths -----------------------------------------------------------

    def _keys(self, path: str, cwd: str | None):
        p = os.path.expanduser(path)
        if not os.path.isabs(p):
            p = os.path.join(cwd or self.repo, p)
        lex = _key(p)
        try:
            real = _key(os.path.realpath(p))
        except (OSError, ValueError):
            real = lex
        return lex, real

    @staticmethod
    def _pathlike(path) -> bool:
        return bool(path) and len(path) <= 2000 and "\n" not in path \
            and "\0" not in path and UNK not in path and PSUB not in path

    def classify(self, path: str, cwd: str | None = None):
        """Return (class, entry) for one path, relative paths against cwd."""
        if not self._pathlike(path):
            return NONE, None
        lex, real = self._keys(path, cwd)
        opened = None
        for e in self.entries:
            for c in (lex, real):
                if any(_under(c, r) for r in e.roots):
                    if any(_under(c, o) for o in e.opens):
                        opened = e
                    else:
                        return SEALED, e
        if opened is not None:
            return OPEN, opened
        for e in self.entries:
            if any(_under(r, real) for r in e.real_roots):
                return ANC_REAL, e
        for e in self.entries:
            if any(_under(r, lex) for r in e.roots):
                return ANC_LINK, e
        return NONE, None

    def parent_of(self, path: str, cwd: str | None = None):
        """Entry whose sealed folder sits directly inside `path`."""
        if not self._pathlike(path):
            return None
        keys = self._keys(path, cwd)
        for e in self.entries:
            if any(k in e.parents for k in keys):
                return e
        return None

    def is_control(self, path: str, cwd: str | None = None) -> bool:
        if not self._pathlike(path):
            return False
        return self._keys(path, cwd)[1] in self.control

    def is_self(self, path: str, cwd: str | None = None) -> bool:
        return self._pathlike(path) and self._keys(path, cwd)[1] == self.self_path

    def is_approved(self, path: str, cwd: str | None = None) -> bool:
        return self._pathlike(path) and self._keys(path, cwd)[1] in self.approved

    # ---- free text -------------------------------------------------------

    def text_trigger(self, text: str):
        """(entry, how) if free text names, can reach, or runs into a folder."""
        t = _norm_text(text)
        for e in self.entries:
            if any(_has_marker(t, m, e.open_names) for m in e.markers):
                return e, "names"
        for e in self.entries:
            if any(_has_marker(t, pm) for pm in e.parent_markers) and any(
                    _has_word(t, leaf, e.open_names) for leaf in e.leaves):
                return e, "reaches"
            for pm in e.parent_markers:  # parent/<wildcard or placeholder>/...
                for m in re.finditer(re.escape(pm) + r"/([^/\s]*)/", t):
                    if re.search(r"[*?\[{$%]|" + UNK, m.group(1)):
                        return e, "reaches"
            for stem in e.stems:
                if re.search(r"(?<![a-z0-9_])" + re.escape(stem), t):
                    return e, "script"
        return None

    def names(self, text: str):
        hit = self.text_trigger(text)
        return hit[0] if hit and hit[1] == "names" else None

    def leaf_in(self, entry, text: str) -> bool:
        t = _norm_text(text)
        return any(_has_word(t, leaf, entry.open_names) for leaf in entry.leaves)

    def mentions_control(self, text: str) -> bool:
        t = _norm_text(text)
        return any(re.search(r"(?<![a-z0-9_])" + re.escape(n), t)
                   for n in self.control_names)

    # ---- verdicts --------------------------------------------------------

    def deny(self, why: str, entry=None, hint: bool = False) -> Decision:
        entry = entry or (self.entries[0] if self.entries else None)
        where = entry.label() if entry is not None else "A sealed location"
        return Decision(DENY, (
            f"Blocked by the sealed-data rule (owner instruction, 2026-10-11). "
            f"{where} is listed in llm_fitting/SEALED_PATHS.json: its contents "
            f"may not be read until the owner gives explicit permission in the "
            f"conversation. {why} "
            + (STRICT_HINT + " " if hint else "")
            + "Allowed there: listing, sizes, checksums, integrity tests such "
            "as `zstd -t`, downloads, and copies to a destination that is "
            "itself listed. Do not look for another route to the same bytes; "
            "if the task needs this data, stop and ask the owner."), entry)

    def check_command(self, cmd: str, cwd: str | None = None) -> Decision:
        return _Run(self, cwd or os.getcwd()).decide(cmd)

    def check_read(self, path: str, cwd: str | None = None) -> Decision:
        kind, e = self.classify(path, cwd)
        if kind == SEALED:
            return self.deny("Reading this file would read sealed contents.", e)
        return Decision()

    def check_tool(self, name: str, tool_input, cwd: str | None) -> Decision:
        cwd = cwd if isinstance(cwd, str) and cwd else os.getcwd()
        ti = tool_input if isinstance(tool_input, dict) else {}
        if name == "Bash":
            return self.check_command(str(ti.get("command", "")), cwd)
        if name in ("Read", "NotebookRead"):
            return self.check_read(str(ti.get("file_path")
                                       or ti.get("notebook_path") or ""), cwd)
        if name == "Grep":
            kind, e = self.classify(str(ti.get("path") or cwd), cwd)
            if kind not in (SEALED, ANC_REAL):
                e = self.names(str(ti.get("glob") or ""))
            if kind in (SEALED, ANC_REAL) or e is not None:
                return self.deny("A content search here would read sealed "
                                 "files.", e)
            return Decision()
        if name in ("Edit", "Write", "NotebookEdit", "MultiEdit"):
            path = str(ti.get("file_path") or ti.get("notebook_path") or "")
            kind, e = self.classify(path, cwd)
            if kind == SEALED:
                return self.deny("Editing or overwriting a file there is "
                                 "not an opaque operation.", e)
            if self.is_control(path, cwd):
                return Decision(ASK, RULE_CHANGE)
            return Decision()
        if name in ("Glob", "Agent", "Task", "Skill", "ToolSearch",
                    "AskUserQuestion", "TodoWrite", "WebSearch"):
            return Decision()
        shellish = bool(re.search(r"terminal|shell|bash|monitor", name, re.I))
        runs_code = bool(re.search(
            r"execut|run_?(code|cell)|javascript|eval|jupyter|kernel|repl",
            name, re.I))
        for key, val in ti.items():
            if key in TEXT_KEYS and not runs_code:
                continue
            for s in _strings(val):
                if shellish and key not in PATH_KEYS:
                    d = self.check_command(s, cwd)
                    if d.verdict != ALLOW:
                        return d
                    continue
                if runs_code:
                    hit = self.text_trigger(s)
                    if hit:
                        return self.deny(
                            f"The tool `{name}` was given code that names or "
                            f"can reach this folder.", hit[0])
                    continue
                cand = s.strip()
                if len(cand) > 2000 or "\n" in cand:
                    continue
                if cand.casefold().startswith("file:"):
                    cand = urllib.parse.unquote(cand[5:])
                    cand = "/" + cand.lstrip("/")
                    if cand.startswith("/localhost/"):
                        cand = cand[10:]
                elif key not in PATH_KEYS and not cand.startswith(
                        ("/", "~", "./", "../")) and not self.names(cand):
                    continue
                kind, e = self.classify(cand, cwd)
                if kind == SEALED:
                    return self.deny(
                        f"The tool `{name}` was given a file there.", e)
        return Decision()


class _Run:
    """One command line: collect its simple commands, then judge them."""

    def __init__(self, reg: Registry, cwd: str):
        self.reg, self.cwd0 = reg, cwd
        self.records, self.subs, self.heredocs = [], [], []
        self.token_trigger = None   # a word resolved into a sealed folder
        self.parent_named = None    # a word or cd named a sealed folder's parent
        self.parse_failed = False
        self.depth = 0

    # ---- reading the line --------------------------------------------------

    def _prepass(self, cmd: str) -> str:
        """Drop comments, lift here-documents out, turn newlines into ';'."""
        out, i, n, quote, word_start = [], 0, len(cmd), None, True
        pending = []  # (delimiter, strip_tabs, placeholder position in out)
        outer, depth = [], 0  # quoting contexts suspended by $( ... )
        while i < n:
            ch = cmd[i]
            if quote != "'" and cmd.startswith("$(", i) \
                    and not cmd.startswith("$((", i):
                outer.append((quote, depth))
                quote, depth, word_start = None, 0, True
                out.append("$(")
                i += 2
            elif quote is None and ch == ")" and depth == 0 and outer:
                quote, depth = outer.pop()
                out.append(")")
                i += 1
            elif quote == "'":
                out.append(ch)
                quote = None if ch == "'" else quote
                i += 1
            elif quote == '"':
                if ch == "\\" and i + 1 < n:
                    out.append(cmd[i:i + 2])
                    i += 2
                    continue
                out.append(ch)
                quote = None if ch == '"' else quote
                i += 1
            elif ch == "\\" and i + 1 < n:
                if cmd[i + 1] != "\n":
                    out.append(cmd[i:i + 2])
                    word_start = False
                i += 2
            elif ch in "'\"":
                quote = ch
                out.append(ch)
                word_start = False
                i += 1
            elif cmd.startswith("<<", i) and not cmd.startswith("<<<", i):
                m = re.match(r"<<(-?)[ \t]*(['\"]?)([A-Za-z_][A-Za-z0-9_]*)\2",
                             cmd[i:])
                body_at = cmd.find("\n", i)
                if not m or body_at < 0:
                    out.append(ch)
                    i += 1
                    continue
                pending.append((m.group(3), bool(m.group(1)), len(out)))
                out.append("")  # filled in when the body is found
                i += m.end()
                word_start = True
            elif ch == "\n":
                i += 1
                while pending:
                    delim, strip, pos = pending.pop(0)
                    lines, j, found = [], i, False
                    while j <= n:
                        k = cmd.find("\n", j)
                        k = n if k < 0 else k
                        line = cmd[j:k]
                        j = k + 1
                        if line.strip() == delim:
                            found = True
                            break
                        lines.append(line)
                        if k >= n:
                            break
                    if not found:
                        out[pos] = "<<" + delim  # not a here-document after all
                        continue
                    self.heredocs.append("\n".join(lines))
                    out[pos] = f" << {_HD_L}{len(self.heredocs) - 1}{_HD_R} "
                    i = j
                out.append(" ; ")
                word_start = True
            elif ch == "#" and word_start:
                while i < n and cmd[i] != "\n":
                    i += 1
            else:
                depth += 1 if ch == "(" else -1 if ch == ")" and depth else 0
                out.append(ch)
                word_start = ch in " \t;|&()"
                i += 1
        for delim, _strip, pos in pending:
            out[pos] = "<<" + delim
        return "".join(out)

    def _lift_subs(self, text: str) -> str:
        """Replace $(…), `…` and <(…) by placeholders, innermost first."""
        for _ in range(400):
            for rx, kind in _SUB_RES:
                m = rx.search(text)
                if m:
                    break
            else:
                return text
            self.subs.append((kind, m.group(1)))
            text = (text[:m.start()] + f"{_SUB_L}{len(self.subs) - 1}{_SUB_R}"
                    + text[m.end():])
        self.parse_failed = True
        return text

    def collect_raw(self, cmd: str, state: dict) -> None:
        self.collect_text(self._lift_subs(self._prepass(cmd)), state)

    def collect_text(self, text: str, state: dict) -> None:
        if self.depth > 12:
            self.parse_failed = True
            return
        try:
            lexer = shlex.shlex(text, posix=True, punctuation_chars=True)
            lexer.whitespace_split = True
            lexer.commenters = ""
            tokens = list(lexer)
        except ValueError:
            self.parse_failed = True
            return
        seg, pending = [], None
        self.depth += 1
        for tok in tokens:
            if tok and all(c in _OPS for c in tok):
                if "<" in tok or ">" in tok:
                    if seg and seg[-1][1] is None and seg[-1][0].isdigit() \
                            and len(seg[-1][0]) == 1:
                        seg.pop()
                    pending = ("herestr" if tok.startswith("<<<") else
                               "heredoc" if tok.startswith("<<") else
                               "in" if ">" not in tok else "out")
                else:
                    if seg:
                        self._record(seg, state)
                    seg, pending = [], None
                continue
            seg.append((tok, pending))
            pending = None
        if seg:
            self._record(seg, state)
        self.depth -= 1

    # ---- words -------------------------------------------------------------

    def _expand(self, tok: str, state: dict) -> list:
        """Possible values of one word; UNK marks what cannot be computed."""
        def sub_placeholder(m):
            kind, body = self.subs[int(m.group(1))]
            inner = {"cwd": state["cwd"], "cwd_unknown": state["cwd_unknown"],
                     "vars": dict(state["vars"])}
            self.collect_text(body, inner)
            return PSUB if kind == "proc" else UNK

        tok = _SUB_TOKEN.sub(sub_placeholder, tok)
        pieces, choices, pos = [], [], 0
        for m in _VAR_RE.finditer(tok):
            pieces.append(tok[pos:m.start()])
            pos = m.end()
            name = m.group(1) or m.group(4)
            if name is None:
                choices.append([UNK])
            elif name in state["vars"]:
                choices.append(state["vars"][name] or [""])
            elif name == "PWD" and not state["cwd_unknown"]:
                choices.append([state["cwd"]])
            elif m.group(2):  # ${NAME:-default}
                choices.append(self._expand(m.group(3) or "", state))
            elif name in _SAFE_ENV and name in os.environ:
                choices.append([os.environ[name]])
            else:
                choices.append([UNK])
        pieces.append(tok[pos:])
        total = 1
        for c in choices:
            total *= len(c)
        if total > 32:
            choices = [[UNK] if len(c) > 1 else c for c in choices]
        out = []
        for combo in itertools.product(*choices):
            out.append("".join(p + v for p, v in zip(pieces, combo + ("",))))
        return out or [""]

    @staticmethod
    def _braces(text: str) -> list:
        if len(text) > 600 or any(c in text for c in "\"'():=\n"):
            return [text]
        out, queue = [], [text]
        while queue and len(out) + len(queue) <= 64:
            cur = queue.pop()
            m = re.search(r"\{([^{}]*,[^{}]*)\}", cur)
            if not m:
                out.append(cur)
                continue
            for alt in m.group(1).split(",")[:16]:
                queue.append(cur[:m.start()] + alt + cur[m.end():])
        return out + queue

    def _classify_glob(self, val: str, cwd: str):
        reg = self.reg
        if not any(c in val for c in "*?["):
            return reg.classify(val, cwd)
        idx = min(i for i, c in enumerate(val) if c in "*?[")
        prefix = val[:idx]
        pdir = prefix if prefix.endswith("/") else os.path.dirname(prefix)
        kind, e = reg.classify(pdir or ".", cwd)
        if kind in (SEALED, OPEN):
            return kind, e
        if kind == NONE:
            return NONE, None
        if "**" in val:  # the shell descends from here; *** follows links
            if kind == ANC_REAL or "***" in val:
                return SEALED, e
            return kind, e
        pat = os.path.expanduser(val)
        if not os.path.isabs(pat):
            pat = os.path.join(cwd or reg.repo, pat)
        try:
            for n, match in enumerate(_glob.iglob(pat)):
                if n > 5000:
                    return SEALED, e
                k2, e2 = reg.classify(match, cwd)
                if k2 == SEALED:
                    return SEALED, e2
        except (OSError, ValueError, re.error):
            return SEALED, e
        return kind, e

    def _arg(self, tok: str, state: dict, redir=None) -> Arg:
        reg, cwd = self.reg, state["cwd"]
        vals = self._expand(tok, state)
        a = Arg(vals[0], redir)
        a.texts, a.raw = vals, tok
        for v in vals:
            if PSUB in v:
                a.psub = True
                continue
            if UNK in v:
                a.unknown = True
                prefix = v.split(UNK, 1)[0]
                pdir = prefix if prefix.endswith("/") else os.path.dirname(prefix)
                if not prefix.startswith("-"):
                    kind, e = reg.classify(pdir or ".", cwd)
                    e = e if kind == SEALED else reg.parent_of(pdir or ".", cwd)
                    if e is not None:  # may land in the sealed folder
                        a.cls, a.entry = SEALED, e
                continue
            if _NUM_RE.match(v) or not reg._pathlike(v):
                continue
            is_flag = len(v) > 1 and v.startswith("-")
            cands = [] if is_flag else [v]
            if "=" in v:
                cands.append(v.split("=", 1)[1])
            for cand in cands:
                for alt in self._braces(cand):
                    kind, e = self._classify_glob(alt, cwd)
                    if kind > a.cls:
                        a.cls, a.entry = kind, e
                    if kind in (ANC_REAL, ANC_LINK):
                        self.parent_named = (self.parent_named
                                             or reg.parent_of(alt, cwd))
                if state["cwd_unknown"] and not is_flag and not os.path.isabs(
                        os.path.expanduser(cand)):
                    a.unknown = True
        if a.cls == SEALED:
            self.token_trigger = self.token_trigger or a.entry
        return a

    # ---- one simple command --------------------------------------------------

    def _record(self, seg, state: dict, indirect: bool = False) -> None:
        reg = self.reg
        toks = list(seg)
        while toks and toks[0][1] is None and toks[0][0] in KEYWORDS:
            toks.pop(0)
        while toks and toks[0][1] is None:
            m = _ASSIGN_RE.match(toks[0][0])
            if not m:
                break
            given = self._arg(m.group(2), state)
            state["vars"][m.group(1)] = given.texts
            toks.pop(0)
        while toks and toks[0][1] is None:
            name = os.path.basename(toks[0][0])
            if name not in WRAPPERS:
                break
            valued = WRAPPERS[name]
            toks.pop(0)
            while toks and toks[0][1] is None and (
                    toks[0][0].startswith("-") or (
                        name == "env" and _ASSIGN_RE.match(toks[0][0]))):
                flag = toks.pop(0)[0]
                m = _ASSIGN_RE.match(flag) if name == "env" else None
                if m:
                    self._arg(m.group(2), state)
                if flag in valued and toks:
                    toks.pop(0)
            if name in ("timeout", "gtimeout") and toks:
                toks.pop(0)
        if not toks:
            return
        cwd = state["cwd"]
        if toks[0][1] is not None:
            prog_text, rest = "", toks
        else:
            named = self._arg(toks[0][0], state) if (
                "/" in toks[0][0] or "$" in toks[0][0]
                or _SUB_L in toks[0][0]) else Arg(toks[0][0])
            prog_text, rest = named.text, toks[1:]
            if named.unknown:
                prog_text = UNK
        prog = os.path.basename(prog_text)

        if prog == "for" and len(rest) >= 2 and rest[1][0] == "in":
            values = []
            for t, _ in rest[2:]:
                values.extend(self._arg(t, state).texts)
            state["vars"][rest[0][0]] = values[:32] if len(values) <= 32 else [UNK]
            rest = rest[2:]
        elif prog in ("read", "mapfile", "readarray"):
            for t, r in rest:
                if r is None and re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", t):
                    state["vars"][t] = [UNK]
        elif prog in ("export", "local", "declare", "typeset", "readonly"):
            for t, _ in rest:
                m = _ASSIGN_RE.match(t)
                if m:
                    state["vars"][m.group(1)] = self._arg(m.group(2), state).texts

        args, bodies = [], []
        for tok, redir in rest:
            if redir == "heredoc":
                m = _HD_TOKEN.search(tok)
                if m:
                    bodies.append(self.heredocs[int(m.group(1))])
                continue
            a = self._arg(tok, state, redir)
            if redir == "herestr":
                bodies.append(a.text)
                continue
            args.append(a)
        raw = [a.text for a in args if a.redir is None]

        if prog in CD:
            target = next((a for a in args if not a.flag), None)
            if prog == "popd" or target is None or target.unknown \
                    or target.text == "-":
                state["cwd_unknown"] = True
            else:
                p = os.path.expanduser(target.text)
                state["cwd"] = os.path.normpath(
                    p if os.path.isabs(p) else os.path.join(cwd, p))
                self._note_cwd(state["cwd"])
        rec = {"prog": prog, "prog_text": prog_text, "args": args, "raw": raw,
               "bodies": bodies, "cwd": cwd, "indirect": indirect,
               "cwd_unknown": state["cwd_unknown"],
               "cwd_kind": reg.classify(cwd, cwd)[0], "inner": False}
        self.records.append(rec)

        inner = {"cwd": state["cwd"], "cwd_unknown": state["cwd_unknown"],
                 "vars": dict(state["vars"])}
        for k in "0123456789@*":
            inner["vars"][k] = [UNK]
        if prog in SHELLS:
            code = None
            for i, a in enumerate(args):
                if a.redir is None and a.flag and "c" in a.text[1:] \
                        and not a.text.startswith("--"):
                    nxt = [b for b in args[i + 1:] if b.redir is None]
                    code = nxt[0] if nxt else None
                    break
            if code is not None and UNK not in _SUB_TOKEN.sub("", code.raw):
                rec["inner"] = True
                self.collect_raw(code.raw, inner)
            elif code is None and bodies and not [
                    a for a in args if a.redir is None and not a.flag]:
                rec["inner"] = True
                for b in bodies:
                    self.collect_raw(b, inner)
        elif prog == "trap":
            code = next((a for a in args if a.redir is None and not a.flag), None)
            if code is not None and UNK not in _SUB_TOKEN.sub("", code.raw):
                rec["inner"] = True
                self.collect_raw(code.raw, inner)
        elif prog == "eval" and not any(a.unknown for a in args):
            rec["inner"] = True
            self.collect_raw(" ".join(raw), inner)  # as the shell re-reads it
            self.collect_raw(" ".join(shlex.quote(r) for r in raw), dict(
                inner, vars=dict(inner["vars"])))    # and word by word
        elif prog in XARGS:
            sub, skip, repl = [], False, "{}"
            words = [a for a in args if a.redir is None]
            for i, r in enumerate(raw):
                if skip:
                    skip = False
                    if raw[i - 1] in ("-I", "-J"):
                        repl = r
                    continue
                if r.startswith("-"):
                    skip = r in ("-n", "-I", "-P", "-L", "-J", "-s", "-E",
                                 "-d", "-j", "--jobs", "-a")
                    continue
                sub = [a.raw for a in words[i:]]
                break
            rec["inner"] = True
            if sub:
                self._record([(t.replace(repl, "$1"), None) for t in sub]
                             + [("$1", None)], inner, indirect=True)
        elif prog in FIND:
            rec["inner"] = True
            for i, r in enumerate(raw):
                if r in ("-exec", "-execdir", "-ok", "-okdir", "-x", "-X",
                         "--exec", "--exec-batch"):
                    words = [a for a in args if a.redir is None]
                    sub = [a.raw for a in words[i + 1:] if a.raw != "+"]
                    if sub:
                        self._record([(t.replace("{}", "$1"), None) for t in sub]
                                     + [("$1", None)], inner, indirect=True)
                    break
            follows = "L" in _short(raw) or "-follow" in raw
            if any(a.cls == ANC_REAL or (a.cls == ANC_LINK and follows)
                   for a in args if not a.flag):
                self.token_trigger = self.token_trigger or next(
                    a.entry for a in args if a.cls in (ANC_REAL, ANC_LINK))
        elif prog in ("tree", "du", "gdu") or (
                prog in ("ls", "gls", "eza", "exa") and "R" in _short(raw)):
            hit = next((a for a in args if a.cls == ANC_REAL and not a.flag), None)
            if hit is not None:
                self.token_trigger = self.token_trigger or hit.entry

    def _note_cwd(self, cwd: str) -> None:
        kind, e = self.reg.classify(cwd, cwd)
        if kind == SEALED:
            self.token_trigger = self.token_trigger or e
        self.parent_named = self.parent_named or self.reg.parent_of(cwd, cwd)

    # ---- the verdict -----------------------------------------------------------

    def decide(self, cmd: str) -> Decision:
        reg = self.reg
        text = self._prepass(cmd)
        whole = text + "\n" + "\n".join(self.heredocs)
        hit = reg.text_trigger(whole)
        self._note_cwd(self.cwd0)
        state = {"cwd": self.cwd0, "cwd_unknown": False, "vars": {}}
        self.collect_text(self._lift_subs(text), state)
        trig, how = (hit if hit else (None, None))
        if trig is None and self.token_trigger is not None:
            trig, how = self.token_trigger, "path"
        if trig is None and self.parent_named is not None and reg.leaf_in(
                self.parent_named, whole):
            trig, how = self.parent_named, "reaches"
        if self.parse_failed:
            if trig is not None:
                return reg.deny(
                    "The command could not be parsed, so it cannot be shown "
                    "to be an opaque operation.", trig, hint=True)
            return Decision()
        ask = None
        for rec in self.records:
            d = self._judge(rec, trig, how)
            if d.verdict == DENY:
                return d
            if d.verdict == ASK and ask is None:
                ask = d
        return ask or Decision()

    def _positionals(self, prog: str, args: list):
        """File-like words of a command, with option values and scripts apart."""
        valued = VALUE_OPTS.get(prog, {})
        words, scripts, skip, script_next = [], [], 0, False
        for a in args:
            if a.redir is not None or a.text == "":
                continue
            if skip:
                skip -= 1
                continue
            if script_next:
                scripts.append(a)
                script_next = False
                continue
            if a.flag and not a.psub:
                opt = a.text.split("=", 1)[0]
                if "=" in a.text:
                    words.append(a)  # --opt=value: the value may be a file
                elif opt in valued:
                    if prog in ("sed", "gsed", "grep", "ggrep", "egrep", "fgrep",
                                "rg", "ag", "ack") and opt in ("-e", "--regexp"):
                        script_next = True
                    else:
                        skip = valued[opt]
                continue
            if a.numeric:
                continue
            words.append(a)
        files = [w for w in words if not w.flag]
        opts = [w for w in words if w.flag]
        short = _short([a.text for a in args if a.redir is None])
        if prog in GREP:
            if not scripts and not short & {"f"} and files:
                scripts.append(files.pop(0))
        elif prog in ("sed", "gsed"):
            if not scripts and "f" not in short and files:
                scripts.append(files.pop(0))
        elif prog in ("awk", "gawk", "mawk"):
            if "f" not in short and files:
                scripts.append(files.pop(0))
        elif prog == "jq":
            if files:
                scripts.append(files.pop(0))
        elif prog == "tr":
            files = []
        return files, opts, scripts

    def _opaque_mode(self, prog: str, raw: list, args: list) -> bool:
        """True if this call of a modal program cannot reveal contents."""
        short, long_ = _short(raw), _long(raw)
        if prog == "wc":
            return bool(short or long_) and short <= {"c"} and long_ <= {"--bytes"}
        if prog in ZSTD:
            if short & {"d", "c"} or long_ & {
                    "--decompress", "--uncompress", "--stdout", "--rm",
                    "--output-dir-flat", "--output-dir-mirror"}:
                return False
            if "-o" in raw and raw.index("-o") + 1 < len(raw):
                dest = raw[raw.index("-o") + 1]
                return any(a.text == dest and a.cls == SEALED for a in args)
            return True
        if prog in OTHERZ:
            return bool(short & {"t", "l"} or long_ & {"--test", "--list"}) \
                and not (short & {"d", "c"} or long_ & {"--decompress", "--stdout"})
        if prog in TAR:
            mode = set(raw[0]) if raw and not raw[0].startswith("-") else set()
            mode |= short
            return ("t" in mode or "--list" in long_) and not (
                mode & set("xcruAO") or long_ & {"--to-stdout", "--to-command",
                                                 "--extract", "--create"})
        if prog == "zipinfo":
            return True
        if prog == "unzip":
            return bool(short & set("lZt")) and not short & set("pc")
        if prog == "cmp":
            return "s" in short or bool(long_ & {"--silent", "--quiet"})
        if prog == "diff":
            return "q" in short or "--brief" in long_
        return False

    def _judge(self, rec: dict, trig, how) -> Decision:
        reg = self.reg
        prog, args, raw, cwd = rec["prog"], rec["args"], rec["raw"], rec["cwd"]
        short, long_ = _short(raw), _long(raw)
        plain = [a for a in args if a.redir is None]
        files, opts, scripts = self._positionals(prog, args)
        fed = [a for a in args if a.redir == "in"]
        first = next((a for a in plain if not a.flag), None)
        runs_self = prog.startswith("python") and first is not None and \
            reg.is_self(first.text, cwd)
        entry = next((a.entry for a in args if a.cls == SEALED and a.entry),
                     None) or trig

        # -- changing the rule itself asks the owner ---------------------------
        def is_ctrl(a):
            return any(reg.is_control(t, cwd) for t in a.texts)
        ctrl_args = [a for a in plain if is_ctrl(a)]
        ctrl_text = reg.mentions_control(" ".join(raw)) or (
            prog not in FILTER and prog not in NOOP and prog != "git"
            and any(reg.mentions_control(b) for b in rec["bodies"]))
        changes = any(is_ctrl(a) for a in args if a.redir == "out")
        if not changes and (ctrl_args or ctrl_text) and not runs_self:
            if prog in ("sed", "gsed") and any(
                    r.startswith("-i") for r in raw):
                changes = True
            elif prog == "tee":
                changes = bool(ctrl_args)
            elif prog in FILTER or prog in MODAL or prog in LIST \
                    or prog in HASH or prog in NOOP or prog in TEST \
                    or prog in CD or prog in TEXT or rec["inner"] or not prog:
                changes = False
            elif prog == "git":
                sub = self._git_sub(plain)
                changes = sub in ("checkout", "restore", "reset", "rm", "mv",
                                  "apply", "stash", "clean", "revert")
            elif prog in ("cp", "gcp", "rsync", "ditto", "install"):
                words = [a for a in plain if not a.flag]
                changes = bool(words) and is_ctrl(words[-1])
            else:
                changes = True

        def done(decision: Decision) -> Decision:
            if decision.verdict == ALLOW and changes:
                return Decision(ASK, RULE_CHANGE)
            return decision

        def deny(why, e=None, hint=True):
            return reg.deny(why, e or entry, hint)

        # -- rules that hold on every line -------------------------------------
        if any(a.redir == "out" and a.cls == SEALED for a in args):
            return deny("Redirecting output into a file there can overwrite "
                        "data.", hint=False)
        if prog in GREP:
            recursive = prog in ("rg", "ag", "ack") or bool(
                short & {"r", "R"}) or bool(
                long_ & {"--recursive", "--dereference-recursive"}) or (
                "recurse" in raw) or "--directories=recurse" in raw
            follows = ("L" in short or "--follow" in long_) if prog == "rg" else (
                "f" in short or "--follow" in long_) if prog == "ag" else (
                "--follow" in long_) if prog == "ack" else (
                bool(short & {"R", "S"}) or "--dereference-recursive" in long_)
            if recursive:
                kinds = [(a.cls, a.entry) for a in files] or [
                    reg.classify(cwd, cwd)]
                for kind, e in kinds:
                    if kind in (SEALED, ANC_REAL) or (kind == ANC_LINK and follows):
                        return deny(
                            f"`{prog}` would search recursively through this "
                            f"folder" + (" by following a link to it; use `-r`, "
                                         "which does not follow links, or name "
                                         "the folders to search."
                                         if kind == ANC_LINK else "."),
                            e, hint=False)
        if prog in TRANSFER or prog in TAR or prog in ("zip", "7z", "7za"):
            words = [a for a in plain if not a.flag]
            if prog == "rsync":
                words, _o, _s = self._positionals("rsync", args)
            sources = words[:-1] if prog in TRANSFER else words
            dest_sealed = prog in TRANSFER and words and words[-1].cls == SEALED
            listing = (prog == "rsync" and ("n" in short or "--dry-run" in long_)) \
                or (prog in TAR and self._opaque_mode(prog, raw, args))
            outer = next((a for a in sources if a.cls == ANC_REAL), None)
            if outer is not None and not dest_sealed and not listing:
                return deny(
                    f"`{prog}` on `{outer.text}` would copy or archive a "
                    f"folder that contains this one. Name the sub-folders "
                    f"you need instead.", outer.entry, hint=False)

        if trig is None:
            return done(Decision())

        # -- the line concerns a sealed folder: default deny --------------------
        if rec["cwd_unknown"]:
            for a in plain:
                if not a.flag and not a.numeric and not os.path.isabs(
                        os.path.expanduser(a.text)):
                    a.unknown = True
        fed_bad = [a for a in fed if not a.safe]
        if not prog:
            if fed_bad:
                return deny("The shell would be fed the contents of a file "
                            "there on its standard input.")
            return done(Decision())
        if prog in CD or runs_self:
            return done(Decision())
        if (prog.startswith("python") or prog in SHELLS) and first is not None \
                and reg.is_approved(first.text, cwd):
            return done(Decision())
        if "/" in rec["prog_text"] and reg.is_approved(rec["prog_text"], cwd):
            return done(Decision())
        if rec["inner"]:
            if prog in FIND and "-delete" in raw:
                return deny("`find -delete` removes files.", hint=False)
            return done(Decision())  # the commands it runs are judged themselves
        if prog in NOOP or prog in TEST:
            if fed_bad:
                return deny(f"`{prog}` would be fed the contents of a file "
                            f"there on its standard input.")
            return done(Decision())
        if prog in LIST or prog in METAW:
            if prog == "file" and (short & set("fzZ") or long_ & {
                    "--files-from", "--uncompress", "--uncompress-noreport"}):
                return deny("`file -f` and `file -z` read contents.")
            if prog == "tree" and "--fromfile" in long_:
                return deny("`tree --fromfile` prints the file it is given.")
            if fed_bad:
                return deny(f"`{prog}` would be fed the contents of a file "
                            f"there on its standard input.")
            return done(Decision())
        if prog in HASH or (prog == "openssl" and first is not None
                            and first.text in ("dgst", "sha256", "md5")):
            if prog == "md5" and "p" in short:
                return deny("`md5 -p` echoes its input.")
            if (("c" in short and prog != "openssl") or "--check" in long_) \
                    and any(not a.safe for a in files):
                return deny(f"`{prog} -c` reads a list of checksums and "
                            f"prints the names it finds in it.")
            return done(Decision())
        if prog in MODAL:
            if self._opaque_mode(prog, raw, args):
                return done(Decision())
            if all(a.safe for a in files + opts) and not fed_bad:
                return done(Decision())
            why = {
                "wc": "`wc` is allowed there only as `wc -c` (a byte count).",
                "cmp": "`cmp` is allowed there only with `-s` (no output).",
                "diff": "`diff` is allowed there only with `-q`, or on the "
                        "output of listings.",
            }.get(prog, f"`{prog}` is allowed there only to test or list an "
                        f"archive, not to decompress or extract it.")
            return deny(why)
        if prog in TRANSFER:
            return done(self._transfer(rec, deny))
        if prog in DOWNLOAD:
            return done(self._download(rec, deny))
        if prog in DESTRUCTIVE:
            if any(not a.safe for a in plain if not a.flag) or \
                    rec["cwd_kind"] == SEALED:
                return deny(f"`{prog}` deletes, overwrites or aliases files "
                            f"there; that is for the owner.", hint=False)
            return done(Decision())
        if prog == "git":
            sub = self._git_sub(plain)
            c_opt = [plain[i + 1] for i, a in enumerate(plain[:-1])
                     if a.text == "-C"]
            paths = [a for a in plain if a.cls == SEALED and not a.flag]
            if sub == "grep" and not short & {"e", "f"}:
                words = [a for a in plain if not a.flag and a.text != "grep"]
                paths = [a for a in paths if a is not (words[0] if words else None)]
            loose = [a for a in plain if not a.flag and not a.safe]
            if any(a.cls == SEALED or a.unknown for a in c_opt) or (
                    sub in ("hash-object", "add", "clean", "mv", "rm", "apply",
                            "am") and (loose or rec["cwd_kind"] == SEALED)) or (
                    sub in ("diff", "grep") and ("--no-index" in raw or paths)) or (
                    paths and sub not in ("commit", "tag", "notes")):
                return deny(f"`git {sub}` would read or change files there.")
            return done(Decision())
        handoff = prog in OTHER_AGENTS or (
            prog in ("node", "npx", "bunx") and any(
                re.search(r"codex|gemini|aider", r, re.I) for r in raw))
        if handoff:
            return Decision(ASK, (
                f"This hands another agent (`{prog}`) a request on a command "
                f"line that concerns a sealed folder. The sealed-data guard "
                f"does not cover what that agent does next, and nothing "
                f"there may be read until the owner says so. Approve only if "
                f"the request cannot lead to a read."), entry)
        if prog in TEXT:
            if prog != "claude" and any(a.cls == SEALED and not a.flag
                                        for a in plain):
                return deny(f"`{prog}` was given a file there.")
            return done(Decision())
        if prog in FILTER:
            bad = [a for a in files + opts if not a.safe]
            if bad or fed_bad:
                which = (bad or fed_bad)[0]
                return deny(
                    f"`{prog}` would read `{which.text.replace(UNK, '…')}`, "
                    f"which is in this folder or cannot be shown not to be."
                    if which.cls == SEALED or not which.unknown else
                    f"`{prog}` is given a file name the guard cannot work "
                    f"out, on a line that concerns this folder.")
            if prog in ("awk", "gawk", "mawk"):
                if "f" in short or any(
                        re.search(r"getline|system|ARGV|ENVIRON|(?<!\|)\|(?!\|)",
                                  s.text) or reg.text_trigger(s.text)
                        for s in scripts):
                    return deny("This `awk` program can read files or run "
                                "commands.")
            if prog in ("sed", "gsed"):
                for s in scripts:
                    m = _SED_SAFE.match(s.text)
                    tail = s.text.rsplit(m.group(5), 1)[-1] if m and m.group(5) else ""
                    if "f" in short or not m or not re.match(r"^[gipI0-9]*\s*$", tail):
                        return deny("This `sed` script is not a plain print, "
                                    "delete or substitution.")
            return done(Decision())
        what = ("a command whose name the guard cannot work out"
                if rec["prog_text"] == UNK else f"`{prog}`")
        if how == "script":
            return deny(
                f"The line names a repository script that reads this folder "
                f"by default, and {what} could run it.", hint=False)
        return deny(f"{what} is not an opaque operation, and this line "
                    f"{'names' if how in ('names', 'path') else 'can reach'} "
                    f"this folder.")

    @staticmethod
    def _git_sub(plain) -> str:
        skip = False
        for a in plain:
            if skip:
                skip = False
                continue
            if a.text in ("-C", "-c"):
                skip = True
                continue
            if not a.flag:
                return a.text
        return ""

    def _transfer(self, rec, deny) -> Decision:
        prog, args, raw = rec["prog"], rec["args"], rec["raw"]
        short, long_ = _short(raw), _long(raw)
        if rec["indirect"]:
            return deny(f"The destination of `{prog}` cannot be checked when "
                        f"it runs through xargs or find.")
        if prog == "rsync":
            words, _o, _s = self._positionals("rsync", args)
        else:
            words = [a for a in args if a.redir is None and not a.flag]
        dest, sources = (words[-1], words[:-1]) if len(words) >= 2 else (None, words)
        if prog != "rsync":
            plain = [a for a in args if a.redir is None]
            for i, a in enumerate(plain):
                if a.text.startswith("--target-directory="):
                    dest, sources = a, words
                elif a.text in ("-t", "--target-directory") and i + 1 < len(plain):
                    dest = plain[i + 1]
                    sources = [w for w in words if w is not dest]
        if dest is None:
            if any(not a.safe for a in sources):
                return deny(f"`{prog}` is given a file there and no "
                            f"destination the guard can check.")
            return Decision()
        dry = prog == "rsync" and ("n" in short or "--dry-run" in long_)
        if any(a.unknown for a in words + [dest]):
            return deny(f"`{prog}` is given a name the guard cannot work "
                        f"out, so where it copies from or to is unknown.")
        from_sealed = any(a.cls == SEALED for a in sources)
        if dest.cls != SEALED:
            if from_sealed and not dry:
                return deny(
                    f"`{prog}` would copy sealed bytes to a place that is "
                    f"not listed. Add the destination to "
                    f"llm_fitting/SEALED_PATHS.json first (an edit the owner "
                    f"approves), then copy.", hint=False)
            return Decision()
        if dry:
            return Decision()
        if prog == "rsync":
            if any(f.startswith(("--delete", "--remove-source-files"))
                   for f in long_):
                return deny("`rsync --delete` and `--remove-source-files` "
                            "remove files there.", hint=False)
            if not from_sealed and "--ignore-existing" not in long_:
                return deny("`rsync` into a sealed folder can overwrite "
                            "files there; use `--ignore-existing`.", hint=False)
            return Decision()
        target = os.path.expanduser(dest.text)
        if not os.path.isabs(target):
            target = os.path.join(rec["cwd"], target)
        try:
            if os.path.isdir(target):
                clash = any(os.path.lexists(os.path.join(
                    target, os.path.basename(a.text.rstrip("/")))) for a in sources)
            else:
                clash = os.path.lexists(target)
        except OSError:
            clash = True
        if clash and "n" not in short:
            return deny(f"`{prog}` would overwrite a file that already "
                        f"exists there.", hint=False)
        return Decision()

    def _download(self, rec, deny) -> Decision:
        reg, prog, raw = self.reg, rec["prog"], rec["raw"]
        short, long_ = _short(raw), _long(raw)
        if rec["indirect"]:
            return deny(f"`{prog}` cannot be checked when it runs through "
                        f"xargs or find.")
        if any("file:" in r.casefold() for r in raw):
            return deny(f"`{prog}` with a file: address reads a local file.")
        if prog == "curl" and (short & set("TdF") or any(
                f.startswith(("--upload", "--data", "--form", "--json"))
                for f in long_)):
            return deny("`curl` would upload or post a file.")
        if prog == "wget" and any(f.startswith(("--post-file", "--body-file"))
                                  for f in long_):
            return deny("`wget` would post a file.")
        outs, prev = OUT_OPTS[prog], ""
        for a in [a for a in rec["args"] if a.redir is None]:
            touchy = a.cls == SEALED or reg.names(a.text)
            if touchy:
                opt = a.text.split("=", 1)[0] if a.flag else prev
                if opt not in outs:
                    return deny(f"`{prog}` is given a file there other than "
                                f"as the place to write or log a download.")
            prev = a.text if a.flag and "=" not in a.text else ""
        return Decision()


def require_released(path: str, list_path: str = DEFAULT_LIST) -> None:
    """Raise SealedDataError if `path` is inside a location still sealed."""
    reg = Registry(list_path)
    kind, e = reg.classify(os.path.abspath(os.path.expanduser(path)), None)
    if kind == SEALED:
        raise SealedDataError(
            f"{path} is inside {e.label()}, listed in {reg.list_path}. It "
            f"may not be read until the owner gives explicit permission.")


def _emit(verdict: str, reason: str) -> None:
    json.dump({"hookSpecificOutput": {
        "hookEventName": "PreToolUse",
        "permissionDecision": verdict,
        "permissionDecisionReason": reason}}, sys.stdout)


def _hook(reg_factory) -> int:
    """Never exits non-zero: a guard that cannot decide refuses drive access."""
    raw = ""
    try:
        raw = sys.stdin.read()
        payload = json.loads(raw)
        d = reg_factory().check_tool(
            str(payload.get("tool_name", "")),
            payload.get("tool_input") or {}, payload.get("cwd"))
        if d.verdict != ALLOW:
            _emit(d.verdict, d.reason)
    except Exception as exc:  # noqa: BLE001 - fail closed on anything at all
        if re.search(r'"tool_name"\s*:\s*"(Edit|Write)"', raw):
            return 0  # let the guard and the list be repaired
        if "/Volumes/" in raw or "data/ssd" in raw:
            _emit(DENY, (
                f"The sealed-data guard failed on this call "
                f"({type(exc).__name__}), so anything that names an external "
                f"data drive is blocked until it is repaired. Check "
                f"llm_fitting/SEALED_PATHS.json and "
                f"scripts/sealed_data_guard.py, or ask the owner."))
    return 0


def _self_check(reg_factory) -> int:
    try:
        reg = reg_factory()
    except (OSError, ValueError, KeyError, TypeError) as exc:
        print(f"FAIL: sealed list unreadable: {exc}")
        return 1
    ids = [e.id for e in reg.entries]
    if len(ids) != len(set(ids)):
        print("FAIL: duplicate ids in the sealed list")
        return 1
    print(f"sealed list: {len(reg.entries)} sealed, "
          f"{len(reg.released)} released, {len(reg.approved)} approved runners")
    settings = os.path.join(reg.repo, ".claude", "settings.json")
    wired = False
    try:
        with open(settings, encoding="utf-8") as fh:
            hooks = json.load(fh).get("hooks", {}).get("PreToolUse", [])
        wired = "sealed_data_guard.py" in json.dumps(hooks)
    except (OSError, ValueError, AttributeError):
        pass
    print("Claude Code hook: " + ("wired in .claude/settings.json" if wired else
          "NOT WIRED — the rule is not enforced mechanically in this checkout"))
    probe = reg.check_command("true", reg.repo)
    return 0 if probe.verdict == ALLOW else 1


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--list-file", default=DEFAULT_LIST)
    ap.add_argument("--cwd", default=None)
    ap.add_argument("--hook", action="store_true")
    ap.add_argument("--check-command")
    ap.add_argument("--check-read")
    ap.add_argument("--list", action="store_true")
    ap.add_argument("--self-check", action="store_true")
    ns = ap.parse_args(argv)

    def factory():
        return Registry(ns.list_file, REPO)

    if ns.hook:
        return _hook(factory)
    if ns.self_check:
        return _self_check(factory)
    reg = factory()
    if ns.list:
        for e in reg.entries:
            print(f"{e.status}: {e.id}")
            for p in e.declared:
                print(f"    {p}")
            for s in e.open_names:
                print(f"    may be read: {s}")
        print("limits: the guard reads the text of one command. It cannot "
              "see what a program opens once running, does not follow names "
              "saved by one command and read by the next, and covers only "
              "agents that call it.")
        return 0
    if ns.check_command is not None:
        d = reg.check_command(ns.check_command, ns.cwd)
    elif ns.check_read is not None:
        d = reg.check_read(ns.check_read, ns.cwd)
    else:
        ap.print_help()
        return 0
    print(d.verdict.upper() + (": " + d.reason if d.reason else ""))
    return 0 if d.verdict == ALLOW else 2


if __name__ == "__main__":
    sys.exit(main())
