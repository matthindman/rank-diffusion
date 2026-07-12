# DRAFT — Amendment A10 (to be appended verbatim to CONFIRMATION_PROTOCOL.md as §15 upon registration)

DRAFT STATUS: this text is frozen as of 2026-07-12 (commit containing this
file). It is INERT — the confirmation battery may not re-run under it —
until BOTH registration conditions in the final section are met. It may not
be edited after the external attestation; any change restarts attestation.

## 15. AMENDMENT A10 (2026-07-12, POST-registration, POST-intake-contact, PRE-confirmatory-outcome): correction of an infeasible data-validation rule — blanket non-negativity replaced by registered per-column semantics; no scientific threshold, stack, band, criterion, or decision rule changed

### Timeline and blindness (exact language)

The automated intake program (`check_extension_panel.py`, A1–A9 form)
accessed the assembled extension panel on 2026-07-12 and printed a single
failure line: `INTAKE FAIL: negative values in extended weekly column
'comment_karma'`. That failure is fully explained by the frozen prefix:
the hash-pinned frozen T=136 weekly baseline (b00ee41f…0041) itself
contains 10,634 negative `comment_karma` cells (frozen daily: 89,477), so
no assembly of the extension, however perfect, could pass the rule as
scoped in code (MODEL_STATUS §2z-o, over-determination argument). **No
extension-specific distribution, summary, count, model statistic, or
E1–E5 outcome was observed by any person or analysis before this
amendment was frozen.** The extension is not claimed to be literally
"unviewed"; the claim — mechanically documented in the archived logs — is
that no extension-specific quantity beyond the failure line existed when
this text was fixed. The extension's negativity rate and distribution
remain unexamined at freeze time.

### The defect

A6.1's intake stop rule "zero negative metrics" was implemented as
non-negativity over EVERY numeric column of both panels. `comment_karma`
is a signed audit field (net votes; negatives are valid platform
behavior), present as negative throughout the frozen baseline. The rule is
therefore infeasible independently of any confirmation outcome — the same
defect class as the A6.2 κ criterion (a registered rule that its own
frozen reference fails), which this protocol already treats as
correctable by dated amendment.

### Facts the correction rests on (frozen panels + code only; archived in `runs/2026-07-12_confirmation/step2b…`, `step2c…` logs)

1. The model never ingests signed karma: daily-panel build sets
   `metric_value = comment_karma.clip(lower=0)`
   (`scripts/data_wrangling/build_reddit_comment_panels.py`); every model
   loader reads only `metric_value` (`minimal_rankdiff.load_panel` column
   list); zero references to `comment_karma`/`submission_karma` in any
   E1–E5 code path (mechanical grep, archived).
2. Daily identity `metric_value == max(comment_karma, 0)` holds exactly on
   all 47,307,511 frozen daily rows. The weekly analogue is false BY
   CONSTRUCTION (clipping precedes weekly aggregation) and is NOT
   registered; weekly integrity is the existing weekly = Σ daily equality,
   which binds `comment_karma` exactly.
3. `submission_karma` and `submission_count` are identically zero in both
   frozen panels (comments-only panel).
4. Clipped mass, measured: absolute negative karma removed by clipping ÷
   total modeled positive-part karma = 831,807 / 39,055,688,181 =
   0.002130% on the frozen daily (negative cells: 0.1891% daily / 0.0754%
   weekly).

### The corrected rule (replaces the [3/6] and [5/6] non-negativity checks ONLY)

Registered per-column semantics, derived from panel construction and
verified against the frozen baseline:

- `metric_value`: no nulls, finite, integral, ≥ 0 (the modeled endpoint).
- `comment_count`: no nulls, finite, integral, ≥ 0.
- `comment_karma`: no nulls, finite, integral, SIGNED (negatives valid).
- `submission_karma`, `submission_count`: identically 0.
- Any OTHER numeric column: no registered semantics → FAIL (fail-closed).
- Nulls anywhere in a panel → FAIL.
- DAILY panels additionally: exact identity
  `metric_value == max(comment_karma, 0)` on every row (binds the audit
  field to the modeled field cell-by-cell — strictly stronger than any
  rate band).
- UNCHANGED VERBATIM: schema equality, frozen-prefix equality,
  complete-week window, duplicate-key rule, calendar-day coverage,
  weekly = Σ daily with exact index-set equality on every metric,
  coverage-log and processing-log rules (A8/A9), zero-parse-errors,
  day guard with frozen history.

REJECTED (recorded so it is not resurrected): an extension-negativity-rate
acceptance band (e.g. [0.5×, 2×] of the frozen rate) — arbitrary new gate
constants that would convert legitimate voting-behavior drift into another
false data failure.

### Mandatory descriptive readouts (REPORTED by the gate; NEVER gates)

- Extension DAILY negative `comment_karma` cell rate (primary — clipping
  is a daily operation) and extension WEEKLY negative-cell rate (describes
  how signed scores aggregate).
- Extension daily clipped-mass ratio, labeled exactly: "absolute negative
  karma removed by clipping ÷ total modeled positive-part karma".

### Estimand declaration (record + paper M&M)

The modeled quantity is **positive-part daily net comment karma**; signed
`comment_karma` is retained solely as an audit field. Frozen-panel
clipping shares as measured above; extension shares reported by the gate.

### Validation requirements (all must hold before the re-attempt)

1. FROZEN SELF-TEST: the amended gate's `--frozen-self-test` mode must
   print PASS on the frozen T=136 weekly + daily panels (the dry-run whose
   absence caused this defect).
2. ADVERSARIAL TESTS, both directions, in the committed suite: legitimate
   negative `comment_karma` PASSES; each of {negative `metric_value`,
   negative count, nonzero submission field, broken daily identity, null
   value, non-integral value, unregistered numeric column} FAILS.
3. DETERMINISM AT RESTART: re-running `build_extension_weekly.py` must
   reproduce the recorded REGISTERED-weekly sha256
   93942240d766e5fa306f26d6c7ebda0c2da0c0aac741b76b956f5d65ce92380d.
4. FULL RESTART: the battery re-executes from Step 0 (preconditions)
   through Step 5, ONCE, in order, zero discretion, under A1–A10. The
   halted 2026-07-12 attempt's archive is preserved unchanged.

### Registration conditions (this amendment is inert until BOTH are met)

1. External attestation (round 9, same reviewer lineage as rounds 4–8)
   that A10 is minimal and outcome-blind — the editor-analog, as this is
   not a formal Registered Report with in-principle acceptance.
2. Owner acknowledgment of the post-intake-contact timeline construction,
   recorded as a dated §5 status line (the A6–A9 mechanism).

Reporting language for the eventual result: "a registered confirmatory
evaluation with one disclosed post-registration, pre-outcome technical
correction" — never "executed exactly as originally preregistered", never
"a literally untouched holdout".
