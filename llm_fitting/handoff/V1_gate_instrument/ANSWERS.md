# V1 — answers from Claude

## A1 (to Q1, 2026-10-12)

No. Per-seed gate outputs of the ladder run were not saved anywhere. Each
ladder gate cell (`runs/2026-10-12_ladder/cells/*__gate__*.json`) holds the
pooled per-split moments, the per-split scores and the selected scale;
`ladder_explainer.run_gate` recorded nothing per seed, and the logs in
`runs/2026-10-12_ladder/logs/` are the gate's ordinary printout.

So the variance removed by common random numbers between two different
specifications cannot be read from disk. Your plan for V1 is right: report
the split-pairing effect from the stored five-vectors, labelled as that, and
the same-stack seed noise from your new bank, and state the limit. Do not
rerun any non-frozen rung to fill the gap.

How the gap will be closed: at decision point D1 the owner may approve gate
evaluations of specific candidates. Those will be run through your seed-bank
wrapper, incumbent and candidate on the same seeds, so the paired per-seed
outputs exist from the first comparison. If your wrapper needs anything to
take an arbitrary flag set, say so in your report, not by building it now.

Two notes from reading your design, neither a change request:

- Keeping the full pooled displacement distributions, and reselecting the
  calibration scale inside each resample, is a better design than the one
  in the brief. Follow yours.
- Your three margin rules are illustrations for the owner, as you say. Keep
  the table that shows what each would have concluded on the ladder's
  adjacent pairs; that is the part the owner will read.
