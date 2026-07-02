Hi Sean,

Sorry for the delay in getting the estimator code to you. I got a little carried away making this more general and more elaborate — and I think it's actually a bigger deal than I initially realized.

For every digital platform there's at least one (usually several) studies claiming that the platform is "approximately power law." If you squint at a log-log plot, the line seems kind of straight, at least near the head.

But if you actually have the data, and you plot the first and second derivative, you see that the slope is monotonically steepening over the entire curve. The decrease is relatively gentle at first, but it balloons the farther down you go. No platform I've looked at is actually power law — they're all concave in log-log.

The problem is that, for most of the platform data we have, we know that some of this steeper slope in the far tail is real, and that at least some is the result of censoring. CrowdTangle, for example, only tracks pages above a certain follower threshold, so the smallest pages drop out. We see the same steepening pattern in fully observed systems (Reddit, equity markets), which tells us it's structural and not just a data artifact — but in partially observed data, you can't cleanly separate the two.

So the approach I've taken is to parameterize alpha, the local Pareto exponent (i.e., the slope of the log-log curve at each rank), and model how it changes as you move deeper into the tail. Specifically, I fit a polynomial in log(alpha) as a function of log-rank, estimated at the censoring boundary where we trust the data.

The key insight — and the reason this works where naive polynomial extrapolation doesn't — is that modeling log(alpha) instead of log(activity) gives you a well-behaved extrapolation. If you fit a cubic to the log-log curve directly, the leading coefficient can go positive and the tail integral blows up to infinity. That actually happens about half the time on real data. But if you fit a polynomial to log(alpha) with non-negative coefficients, any positive-curvature model eventually decays super-exponentially, and the degree-0 case reduces to the familiar condition alpha > 1. You don't get the usual polynomial blow-up problem.

(A note on the non-negativity: the first coefficient, eta_0 >= 0, follows from concavity of the log-log curve — the slope steepens. The higher-order coefficients eta_1, eta_2 >= 0 are modeling choices, not mathematical consequences of concavity. We impose them because they preserve the nested ordering and because fully observed systems like Reddit and equity markets show this pattern empirically. But they are assumptions, not theorems.)

Truncating the polynomial at different degrees gives you a nested family of estimates:

- **Degree 0** is the plain power law — a straight line in log-log, constant alpha. This is the loosest extrapolation, and an upper bound if the tail continues steepening.
- **Degree 1** captures the rate at which the slope steepens (I call this eta). Lower within the same model family.
- **Degree 2** captures the acceleration of that steepening. Lower still within the same family.
- **Degree 3** adds one more derivative. Usually close to degree 2 but occasionally meaningful.

Within this parameterization the estimates are weakly nested: D0 >= D1 >= D2 >= D3, with equality when the added coefficient comes in at zero. The spread between them tells you how sensitive your answer is to the curvature assumptions.

As a concrete example: on the Facebook CrowdTangle data, setting the boundary at rank 5,000 (where we're confident censoring is negligible):

| Model | Unobserved tail (% of total) |
|---|---|
| Power law | ~10% |
| Degree 1 | ~6% |
| Degree 2 | ~6% |
| Degree 3 | ~6% |

So the top 5,000 pages capture roughly 90–94% of total platform engagement, with a best estimate around 94%. The constant-slope extrapolation says "about 10% is missing"; the curvature-corrected models say "more like 6%." That's a meaningful difference if you're trying to estimate total platform activity or compute concentration ratios.

This should generalize to many ranked systems where you observe the top and want to estimate what's below: social media, web traffic, firm revenue, wealth distributions, citation counts. The main requirements are at least ~1,000 ranks in the fitting window and a concave log-log curve (the slope steepens into the tail), which is the generic case for these systems.

I've attached the code and a longer technical writeup with the full derivation. Happy to walk through it whenever works.

Best,
