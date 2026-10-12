# Complete V2 result archive

The study is complete: 1,797 expected checkpoint cells, no failed scientific
seed. `audit.json` verifies cell counts, source/input hashes and execution
wrappers; `predictions.json` scores all 13 predictions.

CSV tables larger than 1 MB are committed as lossless `.csv.gz` files. The
uncompressed originals remain local. No columns or rows were removed.
`table_archive.json` gives byte counts and SHA-256 hashes before/after packing.
To restore the exact names used in the report and plot script:

```bash
/Library/Frameworks/Python.framework/Versions/3.11/bin/python3 llm_fitting/handoff/V2_planted_mechanisms/archive_tables.py --out llm_fitting/runs/2026-10-12_V2_planted_mechanisms --restore
```

- `diagnostic_rates.csv.gz`: all 39,474 condition/statistic rows, including
  means, SDs, seed bands, false-alarm/detection rates, Wilson intervals and
  denominator/missingness fields.
- `diagnostic_pairs.csv.gz`: all 335,529 pair/statistic comparisons, with
  pooled-SD separation, both seed bands and the declared descriptive label.
- `diagnostic_effects.csv.gz`: signed effects and paired Monte Carlo SEs.
- `recovery_*.csv` and `fitted_kappa_*.csv[.gz]`: full recovery and
  misspecification-estimand tables; `annual_cycle_gap_contrast.csv` reports
  the fixed annual signature.
- `compact/*_seeds.jsonl.gz`: complete per-seed diagnostic/estimation evidence,
  retaining raw diagnostic counts. Bulky full fitted-parameter objects are
  omitted; their scientific summaries and source/input hashes remain.
- `compact/*_parameters.json`: complete non-ladder generating parameters,
  with hashes of the local full parameter files. Empirical initial and
  replacement ladders are deliberately local only.
- `attempts/` and logs: completed, failed-before-work, interrupted and
  cancelled execution attempts. Old STARTED records are resolved by explicit
  event files, not silently relabelled successful.

The local `output/v2_plots/` figures were visually checked. Regenerate with
`planted_plot.py --out <this run folder>` after restoring the large tables.
No raw empirical observations, sealed data or figures are committed.
