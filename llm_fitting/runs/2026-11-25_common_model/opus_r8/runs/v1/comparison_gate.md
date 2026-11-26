# Round 8 comparison — `v1`

## Gate v1 (5 x 20, boot 2000; mean relative error, lower is better)

| Panel | Persist. | R4 S2 | R5 S2 | R7 S2 | **R6 S2** | **R8 S2** | dS2 (paired SD, wins) | **R6 S3** | **R8 S3** | dS3 (paired SD, wins) |
|---|---:|---:|---:|---:|---:|---:|---|---:|---:|---|
| FB | 0.147 | 0.084 | 0.093 | 0.083 | 0.089 ± 0.037 | 0.089 ± 0.037 (80%, 3/5) | +0.000 ± 0.000 (0↓/0↑) | 0.091 ± 0.016 | 0.091 ± 0.016 (80%, 5/5) | +0.000 ± 0.000 (0↓/0↑) |
| IG | 0.869 | 0.282 | 0.344 | 0.282 | 0.306 ± 0.232 | 0.306 ± 0.232 (80%, 4/5) | +0.000 ± 0.000 (0↓/0↑) | 0.304 ± 0.230 | 0.304 ± 0.230 (80%, 4/5) | +0.000 ± 0.000 (0↓/0↑) |
| Comments | 0.158 | 0.134 | 0.096 | 0.118 | 0.113 ± 0.035 | 0.120 ± 0.059 (100%, 5/5) | +0.007 ± 0.031 (2↓/2↑) | 0.124 ± 0.057 | 0.087 ± 0.056 (100%, 5/5) | -0.037 ± 0.022 (4↓/0↑) |
| Submissions | 0.165 | 0.263 | 0.196 | 0.209 | 0.178 ± 0.066 | 0.191 ± 0.042 (80%, 3/5) | +0.013 ± 0.030 (1↓/3↑) | 0.198 ± 0.079 | 0.260 ± 0.129 (80%, 1/5) | +0.062 ± 0.106 (1↓/3↑) |

Per-origin moment errors (signed: + = model above data) for the Reddit panels:

**Comments S2**

| T0 | R6 total | R8 total | R6 moments | R8 moments |
|---:|---:|---:|---|---|
| 53 | 0.162 | 0.162 | dRank1:+0.17 dRank4:+0.12 dRank13:+0.18 RACF1:+0.04 coll1:-0.38 coll5:+0.20 coll20:-0.04 | dRank1:+0.17 dRank4:+0.12 dRank13:+0.18 RACF1:+0.04 coll1:-0.38 coll5:+0.20 coll20:-0.04 |
| 80 | 0.122 | 0.138 | dRank1:+0.00 dRank4:+0.00 dRank13:+0.18 RACF1:+0.17 coll1:-0.47 coll5:+0.01 coll20:-0.02 | dRank1:+0.00 dRank4:+0.00 dRank13:+0.18 RACF1:+0.18 coll1:-0.51 coll5:-0.07 coll20:-0.03 |
| 106 | 0.136 | 0.198 | dRank1:+0.00 dRank4:-0.11 dRank13:-0.08 RACF1:-0.04 coll1:-0.51 coll5:-0.19 coll20:+0.02 | dRank1:+0.00 dRank4:-0.11 dRank13:-0.15 RACF1:-0.04 coll1:-0.88 coll5:-0.18 coll20:-0.02 |
| 133 | 0.078 | 0.049 | dRank1:+0.17 dRank4:+0.00 dRank13:+0.08 RACF1:+0.10 coll5:-0.08 coll20:+0.04 | dRank1:+0.00 dRank4:-0.11 dRank13:-0.08 RACF1:+0.04 coll5:-0.02 coll20:+0.05 |
| 160 | 0.069 | 0.055 | dRank1:+0.17 dRank4:+0.00 dRank13:+0.08 RACF1:+0.08 coll5:-0.04 coll20:+0.06 | dRank1:+0.00 dRank4:-0.11 dRank13:+0.00 RACF1:+0.11 coll5:-0.05 coll20:+0.06 |

**Comments S3**

| T0 | R6 total | R8 total | R6 moments | R8 moments |
|---:|---:|---:|---|---|
| 53 | 0.159 | 0.159 | dRank1:+0.17 dRank4:+0.12 dRank13:+0.09 RACF1:-0.04 coll1:+0.52 coll5:+0.15 coll20:-0.02 | dRank1:+0.17 dRank4:+0.12 dRank13:+0.09 RACF1:-0.04 coll1:+0.52 coll5:+0.15 coll20:-0.02 |
| 80 | 0.194 | 0.138 | dRank1:+0.00 dRank4:+0.12 dRank13:+0.27 RACF1:+0.23 coll1:+0.61 coll5:+0.10 coll20:-0.02 | dRank1:+0.00 dRank4:+0.12 dRank13:+0.18 RACF1:+0.14 coll1:+0.41 coll5:+0.09 coll20:-0.02 |
| 106 | 0.152 | 0.092 | dRank1:+0.00 dRank4:+0.00 dRank13:+0.00 RACF1:+0.07 coll1:+0.88 coll5:-0.12 coll20:-0.01 | dRank1:+0.00 dRank4:-0.11 dRank13:-0.08 RACF1:-0.02 coll1:+0.28 coll5:-0.13 coll20:+0.04 |
| 133 | 0.044 | 0.021 | dRank1:+0.17 dRank4:+0.00 dRank13:+0.00 RACF1:+0.01 coll5:-0.03 coll20:+0.05 | dRank1:+0.00 dRank4:+0.00 dRank13:+0.00 RACF1:+0.04 coll5:-0.04 coll20:+0.05 |
| 160 | 0.072 | 0.026 | dRank1:+0.17 dRank4:+0.00 dRank13:+0.08 RACF1:+0.10 coll5:-0.04 coll20:+0.05 | dRank1:+0.00 dRank4:+0.00 dRank13:+0.00 RACF1:+0.06 coll5:-0.04 coll20:+0.06 |

**Submissions S2**

| T0 | R6 total | R8 total | R6 moments | R8 moments |
|---:|---:|---:|---|---|
| 53 | 0.206 | 0.206 | dRank1:+0.33 dRank4:+0.20 dRank13:+0.43 RACF1:+0.15 coll5:-0.11 coll20:+0.02 | dRank1:+0.33 dRank4:+0.20 dRank13:+0.43 RACF1:+0.15 coll5:-0.11 coll20:+0.02 |
| 80 | 0.226 | 0.242 | dRank1:+0.33 dRank4:+0.25 dRank13:+0.50 RACF1:+0.21 coll5:+0.06 coll20:-0.00 | dRank1:+0.33 dRank4:+0.25 dRank13:+0.50 RACF1:+0.20 coll5:+0.15 coll20:-0.02 |
| 106 | 0.048 | 0.115 | dRank1:+0.00 dRank4:+0.00 dRank13:+0.00 RACF1:+0.07 coll5:+0.19 coll20:-0.02 | dRank1:+0.00 dRank4:+0.20 dRank13:+0.12 RACF1:+0.08 coll5:+0.26 coll20:-0.03 |
| 132 | 0.205 | 0.185 | dRank1:+0.00 dRank4:+0.00 dRank13:+0.12 RACF1:+0.03 coll1:-1.00 coll5:-0.22 coll20:+0.07 | dRank1:+0.00 dRank4:+0.00 dRank13:+0.00 RACF1:+0.06 coll1:-1.00 coll5:-0.15 coll20:+0.09 |
| 159 | 0.206 | 0.207 | dRank1:+0.00 dRank4:+0.20 dRank13:+0.25 RACF1:+0.12 coll1:-0.59 coll5:-0.08 coll20:+0.20 | dRank1:+0.00 dRank4:+0.40 dRank13:+0.38 RACF1:+0.18 coll1:-0.04 coll5:-0.24 coll20:+0.21 |

**Submissions S3**

| T0 | R6 total | R8 total | R6 moments | R8 moments |
|---:|---:|---:|---|---|
| 53 | 0.313 | 0.313 | dRank1:+0.33 dRank4:+0.40 dRank13:+0.86 RACF1:+0.20 coll5:+0.04 coll20:+0.05 | dRank1:+0.33 dRank4:+0.40 dRank13:+0.86 RACF1:+0.20 coll5:+0.04 coll20:+0.05 |
| 80 | 0.177 | 0.427 | dRank1:+0.00 dRank4:+0.25 dRank13:+0.50 RACF1:+0.20 coll5:+0.10 coll20:+0.00 | dRank1:+0.33 dRank4:+0.75 dRank13:+1.00 RACF1:+0.28 coll5:+0.17 coll20:+0.03 |
| 106 | 0.088 | 0.126 | dRank1:+0.00 dRank4:+0.00 dRank13:+0.00 RACF1:+0.03 coll5:+0.47 coll20:-0.02 | dRank1:+0.00 dRank4:+0.00 dRank13:-0.12 RACF1:-0.01 coll5:+0.60 coll20:-0.02 |
| 132 | 0.154 | 0.092 | dRank1:+0.00 dRank4:+0.20 dRank13:+0.38 RACF1:+0.12 coll1:+0.30 coll5:+0.01 coll20:+0.08 | dRank1:+0.00 dRank4:+0.00 dRank13:+0.12 RACF1:+0.00 coll1:-0.42 coll5:+0.02 coll20:+0.08 |
| 159 | 0.259 | 0.344 | dRank1:+0.00 dRank4:+0.20 dRank13:+0.25 RACF1:+0.15 coll1:+0.90 coll5:-0.08 coll20:+0.23 | dRank1:+0.00 dRank4:+0.20 dRank13:+0.25 RACF1:+0.10 coll1:+1.59 coll5:-0.06 coll20:+0.22 |


