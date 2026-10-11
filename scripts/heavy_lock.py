#!/usr/bin/env python3
"""Run heavy jobs a few at a time across agents and working copies.

    python scripts/heavy_lock.py -- <command> [args...]

Two agents working in separate working copies share one machine (10 cores,
16 GB). A gate evaluation at registered precision takes 4 to 22 minutes of
one core and several GB on the large panels. This wrapper waits for a free
slot, runs the command, and frees the slot when the command ends, however it
ends: the slot is an operating-system file lock, so a killed job cannot
leave it held.

Slots: RANKDIFF_HEAVY_SLOTS (default 3). Lock files: /tmp/rankdiff-heavy.N.
Use it for every gate evaluation and for any job longer than about five
minutes. See llm_fitting/coordination/PROTOCOL.md.
"""
import fcntl
import os
import subprocess
import sys
import time


def main() -> int:
    cmd = sys.argv[1:]
    if cmd and cmd[0] == "--":
        cmd = cmd[1:]
    if not cmd:
        print(__doc__)
        return 2
    slots = int(os.environ.get("RANKDIFF_HEAVY_SLOTS", "3"))
    base = os.environ.get("RANKDIFF_HEAVY_LOCK", "/tmp/rankdiff-heavy")
    handles = [open(f"{base}.{i}", "a+") for i in range(slots)]
    told = False
    while True:
        for fh in handles:
            try:
                fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                continue
            fh.seek(0)
            fh.truncate()
            fh.write(f"pid {os.getpid()} since {time.strftime('%F %T')} "
                     f"in {os.getcwd()} :: {' '.join(cmd)}\n")
            fh.flush()
            try:
                return subprocess.call(cmd)
            finally:
                fh.seek(0)
                fh.truncate()
        if not told:
            holders = []
            for fh in handles:
                fh.seek(0)
                holders.append(fh.read().strip() or "unknown")
            print("waiting for a heavy-job slot; held by:\n  "
                  + "\n  ".join(holders), file=sys.stderr, flush=True)
            told = True
        time.sleep(15)


if __name__ == "__main__":
    sys.exit(main())
