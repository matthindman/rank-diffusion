
## reddit_comments_ext  (K = 12500, blocks of 52 weeks; shape read at [195, 391, 781, 1562, 3125, 6250] against the top-K mean)

Rms error of the predicted change of shape over those cut-offs, log units over the step (lower is better):

| case | origin → target | transitions from | no change | repeat last change | empirical | moments | smooth | worst slope error: empirical, persistence |
|---|---|---|---|---|---|---|---|---|
| one step, out of sample | 1→2 | [0] | 0.0846 | 0.2786 | 0.2634 | 0.2651 | 0.5313 | 14%, 4% |
| one step, in sample | 1→2 | [1] | 0.0846 | 0.2786 | 0.0031 | 0.0087 | 0.0364 | 3%, 4% |
| one step, out of sample | 2→3 | [1] | 0.0214 | 0.0802 | 0.0747 | 0.0798 | 0.0925 | 5%, 10% |
| one step, in sample | 2→3 | [2] | 0.0214 | 0.0802 | 0.0081 | 0.0174 | 0.1172 | 5%, 10% |
| two steps, out of sample | 1→3 | [0, 0] | 0.0939 | — | 0.5607 | 0.5754 | 1.1029 | 25%, 15% |
| two steps, out of sample | 1→3 | [0, 1] | 0.0939 | — | 0.3008 | 0.3190 | 0.6014 | 13%, 15% |

Observed and predicted (empirical) change of shape, log units, by cut-off:

- one step, out of sample, 1→2 with [0]: 195: obs -0.143, pred -0.446 (sd 0.037); 391: obs -0.112, pred -0.421 (sd 0.022); 781: obs -0.078, pred -0.377 (sd 0.014); 1562: obs -0.053, pred -0.323 (sd 0.012); 3125: obs -0.029, pred -0.253 (sd 0.008); 6250: obs -0.022, pred -0.148 (sd 0.004)
- one step, out of sample, 2→3 with [1]: 195: obs -0.031, pred -0.134 (sd 0.031); 391: obs -0.018, pred -0.104 (sd 0.027); 781: obs +0.010, pred -0.073 (sd 0.020); 1562: obs +0.020, pred -0.048 (sd 0.013); 3125: obs +0.024, pred -0.031 (sd 0.009); 6250: obs +0.019, pred -0.017 (sd 0.005)
- two steps, out of sample, 1→3 with [0, 0]: 195: obs -0.174, pred -0.834 (sd 0.051); 391: obs -0.130, pred -0.783 (sd 0.033); 781: obs -0.068, pred -0.703 (sd 0.022); 1562: obs -0.033, pred -0.607 (sd 0.016); 3125: obs -0.005, pred -0.472 (sd 0.012); 6250: obs -0.003, pred -0.274 (sd 0.006)
- two steps, out of sample, 1→3 with [0, 1]: 195: obs -0.174, pred -0.538 (sd 0.044); 391: obs -0.130, pred -0.482 (sd 0.034); 781: obs -0.068, pred -0.404 (sd 0.023); 1562: obs -0.033, pred -0.335 (sd 0.018); 3125: obs -0.005, pred -0.252 (sd 0.012); 6250: obs -0.003, pred -0.147 (sd 0.006)

Smooth transitions (mean and log variance of the change, linear in log2 rank/K; logit attrition; entrants):

- 0→1: mean +0.573 +0.1322·u; log var -0.498 +0.167·u; logit q -2.50 +0.56·u; entrants 4804 at +0.21 ± 1.34 from the K-th level
- 1→2: mean +0.042 -0.0015·u; log var -0.521 +0.171·u; logit q -2.82 +0.71·u; entrants 4956 at -0.17 ± 1.33 from the K-th level
- 2→3: mean -0.033 -0.0261·u; log var -0.468 +0.117·u; logit q -2.83 +0.56·u; entrants 4249 at -0.52 ± 1.24 from the K-th level

## reddit_submissions_long  (K = 5000, blocks of 52 weeks; shape read at [78, 156, 312, 625, 1250, 2500] against the top-K mean)

Rms error of the predicted change of shape over those cut-offs, log units over the step (lower is better):

| case | origin → target | transitions from | no change | repeat last change | empirical | moments | smooth | worst slope error: empirical, persistence |
|---|---|---|---|---|---|---|---|---|
| one step, out of sample | 1→2 | [0] | 0.1881 | 0.0649 | 0.0621 | 0.0564 | 0.2003 | 17%, 16% |
| one step, in sample | 1→2 | [1] | 0.1881 | 0.0649 | 0.0057 | 0.0334 | 0.0266 | 3%, 16% |
| one step, out of sample | 2→3 | [1] | 0.1328 | 0.0562 | 0.0360 | 0.0747 | 0.0361 | 7%, 13% |
| one step, in sample | 2→3 | [2] | 0.1328 | 0.0562 | 0.0073 | 0.0312 | 0.0574 | 7%, 13% |
| two steps, out of sample | 1→3 | [0, 0] | 0.3207 | — | 0.1082 | 0.1113 | 0.4468 | 26%, 31% |
| two steps, out of sample | 1→3 | [0, 1] | 0.3207 | — | 0.0641 | 0.1008 | 0.2331 | 11%, 31% |

Observed and predicted (empirical) change of shape, log units, by cut-off:

- one step, out of sample, 1→2 with [0]: 78: obs -0.288, pred -0.179 (sd 0.060); 156: obs -0.244, pred -0.210 (sd 0.043); 312: obs -0.192, pred -0.221 (sd 0.028); 625: obs -0.150, pred -0.202 (sd 0.017); 1250: obs -0.089, pred -0.154 (sd 0.011); 2500: obs -0.051, pred -0.100 (sd 0.006)
- one step, out of sample, 2→3 with [1]: 78: obs -0.207, pred -0.257 (sd 0.059); 156: obs -0.171, pred -0.217 (sd 0.043); 312: obs -0.136, pred -0.176 (sd 0.027); 625: obs -0.109, pred -0.132 (sd 0.016); 1250: obs -0.058, pred -0.084 (sd 0.009); 2500: obs -0.016, pred -0.035 (sd 0.005)
- two steps, out of sample, 1→3 with [0, 0]: 78: obs -0.495, pred -0.379 (sd 0.074); 156: obs -0.414, pred -0.406 (sd 0.053); 312: obs -0.328, pred -0.419 (sd 0.038); 625: obs -0.258, pred -0.381 (sd 0.027); 1250: obs -0.147, pred -0.291 (sd 0.019); 2500: obs -0.067, pred -0.180 (sd 0.009)
- two steps, out of sample, 1→3 with [0, 1]: 78: obs -0.495, pred -0.443 (sd 0.079); 156: obs -0.414, pred -0.433 (sd 0.057); 312: obs -0.328, pred -0.394 (sd 0.042); 625: obs -0.258, pred -0.337 (sd 0.027); 1250: obs -0.147, pred -0.233 (sd 0.017); 2500: obs -0.067, pred -0.129 (sd 0.008)

Smooth transitions (mean and log variance of the change, linear in log2 rank/K; logit attrition; entrants):

- 0→1: mean +0.425 +0.0758·u; log var -0.461 +0.158·u; logit q -3.59 +0.34·u; entrants 1941 at +0.26 ± 0.88 from the K-th level
- 1→2: mean -0.055 +0.0197·u; log var -0.545 +0.188·u; logit q -3.08 +0.60·u; entrants 1958 at -0.18 ± 0.86 from the K-th level
- 2→3: mean -0.173 +0.0043·u; log var -0.592 +0.191·u; logit q -3.03 +0.73·u; entrants 1796 at -0.50 ± 0.77 from the K-th level
