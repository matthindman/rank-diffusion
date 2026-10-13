# V4 execution dependency

The runner and tests are prepared. The native study waits for the owner to
start `launch_study.sh` in a user-owned Terminal. Other Claude processes
were observed on the machine.

AGENTS.md, Environment: "Long runs: `python -u`, wrapped in `caffeinate
-dims`, from a user-owned Terminal when another agent session is active;
if you cannot tell, give the owner the command."

The exact launch command is in REPORT.md. No change to Claude-owned model
files, statistics or the protocol is requested. V4 remains IN PROGRESS;
after the grid completes, Codex must inspect the complete tables, finish
the report and record addendum, and commit the recovery map before opening
step 1's empirical results.

## Resolved 2026-10-13

The owner explicitly instructed Codex to start the study in Terminal.
Codex launched `launch_study.sh` in native Terminal window 23047, run stamp
`20261013T085104`, and verified an active preparation worker under
caffeinate and the shared heavy-job lock. No owner action is pending for
launch. Grid completion, complete-result review and the committed recovery
map remain pending.

## Closed 2026-10-14

All 1,200 cells and the automatic report completed successfully. The
complete-result review, scored predictions and final record addendum are
in REPORT, COMPLETION_TABLES and STATUS_ENTRY. No launch dependency or
owner decision remains. No protected-file hook is requested. The recovery
map is handed over on this task branch; empirical M1 results remain
unopened. Claude's canonical-record integration remains its assigned role.
