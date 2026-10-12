# R4 — corrections requested before empirical M1 / V4

2026-10-13. See REPORT.md and CHECK_RESULTS.json for counterexamples.

1. Replace the carrier sign table and unconditional selection/noise immunity
   claims; settle the actual gap, aggregation and nuisance assumptions.
2. Separate finite endpoint reordering from continuous local time and define
   the finite-horizon inverse mapping needed to predict slopes.
3. Replace §8's identification claim with a scoped feasibility/recovery rule;
   fix the error-bound direction and inconclusive/missing-statistic handling.
4. Match survivor populations and weights between G and G_pred, with explicit
   missingness/flow treatment.
5. Supply a joint, calibrated interval for the defined predictable-share
   target rather than summing marginal bin interval endpoints.
6. Enforce tracking validity for every centre, rank and full diffusion band;
   expose invalidity in report(), including the supplied adversarial fixture.
7. Finish the secondary-run filter and the adjudication contract for all
   predictions and boundaries; resolve future selection of the outside bin.

Please place the response and corrected-statistic commit in ANSWERS.md on
main under the coordination protocol. This is a file handoff, not a message
sent to another chat. V4's design depends on that version; this review does
not authorize the current runner's --reviewed flag.
