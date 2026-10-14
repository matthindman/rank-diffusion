# Source under review: an external result (GPT-6 pro), supplied by the owner

_Pasted by the owner into the Claude Code conversation on 2026-10-14 with the
words "Here is a result from GPT-6 pro:". Reproduced verbatim below. The
question it answers was not pasted; its parameter box is inferred from the
text in `REVIEW.md`. The archive it links (`volatility_response_code.zip`,
a sandbox path) was not supplied and has not been seen._

---

# UNRESOLVED

**The requested practical separation result is not established.** I have not proved 80% rejection power against \(\beta\leq-0.0004\), either uniformly or on a materially sized nuisance region. Nor have I constructed a null/alternative pair of complete count laws proving that such power is impossible.

There is, however, a substantive finite-sample result: **a count-only confidence construction with uniform 95% coverage, and an explicit level-.05 test of \(\beta=0\), under the entire primary box.** It retains arbitrary named heterogeneity, unknown nonzero heterogeneous OU noise, unknown initial states, seasonality, daily integration, and zeros.

The construction tests departures from a Gaussian symmetry that holds **within every name under \(\beta=0\)**. It does not estimate state labels or regress movement on rank. The principal observation result is a rigorously bounded transfer from actual daily counts to that symmetry. For the combined annual score defined below, its null expectation has absolute value at most

\[
\boxed{0.001838127.}
\]

That bound includes Poisson low-count error and the non-Gaussian effect of daily integration. Seasonality is handled by profiling a common phase, not by assuming it known.

The remaining gap is **alternative count-law separation and power**, not the validity of this null calibration. The resulting confidence set is conservative and symmetric in \(\beta\); it does not determine the sign of a detected response.

---

## 1. Experiment, calendar, and theorem

All probabilities below are conditional on arbitrary fixed nuisance arrays satisfying the question’s boxes. No distribution of types, independence of type and starting size, or stationarity of \(X_i\) is imposed.

Let

\[
L=\lfloor T/3\rfloor,\qquad H=3L.
\]

Thus the construction uses days \(0,\ldots,362\) at \(T=365\), and all 180 days at \(T=180\). The last two annual observations are omitted by a predetermined rule.

Define daily weights

\[
w_d=
\begin{cases}
L^{-1},&0\leq d<L,\\
-2L^{-1},&L\leq d<2L,\\
L^{-1},&2L\leq d<3L.
\end{cases}
\]

Write \(w(t)=w_d\) on day \(d\). These weights satisfy

\[
\int_0^H w(t)\,dt=0,\qquad
\int_0^H t\,w(t)\,dt=0,
\qquad
\sum_d |w_d|=4,\qquad
\sum_d w_d^2=\frac6L.
\]

Let

\[
k(t)=\int_t^H w(u)\,du
=
\begin{cases}
-t/L,&0\leq t<L,\\
2t/L-3,&L\leq t<2L,\\
3-t/L,&2L\leq t\leq3L.
\end{cases}
\]

Then

\[
\int_0^H k(t)\,dt=0,\qquad
\int_0^H k(t)^2\,dt=L,\qquad |k(t)|\leq1.
\]

### The count transform

For real \(\tau\) and integer \(c\geq0\), define

\[
f_\tau(c)=\frac{\Gamma(c+1)}{\Gamma(c+1-i\tau)}.
\]

For the two fixed frequencies \(\gamma\in\{1,3\}\), compute

\[
U_{i,\gamma}
=
\prod_{d=0}^{H-1} f_{\gamma w_d}(C_{id}).
\tag{1}
\]

This is a deterministic statistic of the original counts. No thinning or artificial randomization is used. In particular, \(f_\tau(0)\) is finite, so zeros require no special treatment.

Define

\[
M_\tau=
\left(\frac{\sinh(\pi\tau)}{\pi\tau}\right)^{1/2},
\qquad M_0=1,
\]

and

\[
M_\gamma^{(L)}
=
M_{\gamma/L}^{\,2L}M_{2\gamma/L}^{\,L}.
\tag{2}
\]

Then \(|U_{i,\gamma}|\leq M_\gamma^{(L)}\).

### The unknown seasonal phase

Let

\[
m(c_1,c_2)=\int_0^H k(t)s(t)\,dt.
\]

Its exact allowable range is the interval

\[
\mathcal M=[-m_{\max},m_{\max}],
\]

where

\[
m_{\max}
=
0.0003\left(
\left|\int_0^H k(t)\sin(2\pi t/365)\,dt\right|
+
\left|\int_0^H k(t)\cos(2\pi t/365)\,dt\right|
\right).
\tag{3}
\]

For each candidate \(m\in\mathcal M\), form two whole-name scores:

\[
S_{i,A}(m)=\operatorname{Im}\!\left(e^{-im}U_{i,1}\right),
\]

\[
S_{i,B}(m)=
\operatorname{Im}\!\left(e^{-3im}U_{i,3}\right)
-\frac32\operatorname{Im}\!\left(e^{-im}U_{i,1}\right).
\tag{4}
\]

Their deterministic ranges are

\[
|S_{i,A}(m)|\leq R_A=M_1^{(L)},
\]

\[
|S_{i,B}(m)|\leq R_B=M_3^{(L)}+\frac32M_1^{(L)}.
\tag{5}
\]

### The proved result

**Theorem.** For every positive integer \(N\), either declared horizon, every allowable seasonal coefficient pair, every allowable fixed nuisance array, and every \(\beta\) in its specified interval, the confidence set in Section 4 satisfies

\[
\Pr_\theta\{\beta\in I(C)\}\geq .95.
\tag{6}
\]

Under \(\beta=0\), for the true \(m=m(c_1,c_2)\), each name separately satisfies

\[
|\mathbb E_\theta S_{i,A}(m)|\leq\epsilon_A,
\qquad
|\mathbb E_\theta S_{i,B}(m)|\leq\epsilon_B,
\tag{7}
\]

with the following outward-rounded bounds:

| Calendar | \(H\) | \(m_{\max}\) upper bound | \(\epsilon_A\) upper bound | \(\epsilon_B\) upper bound |
|---|---:|---:|---:|---:|
| \(T=365\) | 363 | 0.043778 | 0.000284901 | **0.001838127** |
| \(T=180\) | 180 | 0.016597 | 0.000050272 | **0.000949713** |

The test \(Q(C)=\mathbf1\{0\notin I(C)\}\) therefore obeys

\[
\sup_{\theta:\beta=0}\Pr_\theta\{Q(C)=1\}\leq .05.
\tag{8}
\]

These are coverage and size results. **The theorem contains no power assertion.**

For filtrations, let \(\mathscr F_t\) be the completed latent filtration generated by the Brownian drivers and stationary initial OU variables. Append the conditionally independent count draws as specified in the question. The testing argument uses the different filtration

\[
\mathscr G_j=\sigma(C_1,\ldots,C_j),
\]

where \(C_i\) denotes an entire named history. Independence is used only across names.

---

## 2. Why the null symmetry survives permanent heterogeneity

Put \(Y_i=X_i+Z_i\), and consider the unobserved linear contrast

\[
F_i=\int_0^H w(t)Y_i(t)\,dt.
\]

Under \(\beta=0\),

\[
X_i(t)=X_i(0)+b_it+\int_0^t s(u)\,du+\sqrt{\alpha_i}W_i(t).
\]

Integration by parts gives

\[
F_i
=
m(c_1,c_2)
+
\sqrt{\alpha_i}\int_0^H k(t)\,dW_i(t)
+
\int_0^H w(t)Z_i(t)\,dt.
\tag{9}
\]

The starting level and named drift cancel exactly. The remaining random terms are independent, centered Gaussian variables. Consequently,

\[
F_i-m\ \text{is centered Gaussian}
\]

for **each name**, regardless of its \(\alpha_i,v_i,\kappa_i,b_i,X_i(0)\).

Thus

\[
\mathbb E_\theta\sin\{\gamma(F_i-m)\}=0
\qquad(\beta=0).
\tag{10}
\]

This is not a cross-sectional Gaussianity assumption. The roster can be an arbitrary collection of different Gaussian variances. Equation (10) holds name by name before averaging.

Unknown OU parameters therefore do not produce a null mean in this ideal linear contrast. Their variances still affect sampling precision and power.

The observation problem is that

\[
\log\Lambda_{id}
\neq
\log100+\int_d^{d+1}Y_i(t)\,dt.
\]

The next section carries that discrepancy explicitly.

---

## 3. Proof of the count and daily-integration bounds

### 3.1 An exact bounded Poisson transform

For \(K\sim\operatorname{Poisson}(\lambda)\),

\[
\begin{aligned}
\mathbb E f_\tau(K)
&=
e^{-\lambda}\sum_{k=0}^\infty
\frac{\lambda^k}{\Gamma(k+1-i\tau)}\\
&=
\lambda^{i\tau}
\left[
1-\frac{\Gamma(-i\tau,\lambda)}{\Gamma(-i\tau)}
\right].
\end{aligned}
\tag{11}
\]

The series identity is the incomplete-gamma expansion in DLMF §8.7. I checked that source directly; the application to the Poisson expectation is the displayed calculation. [dlmf.nist.gov](https://dlmf.nist.gov/8.7)

Since

\[
\Gamma(-i\tau,\lambda)
=
\int_\lambda^\infty e^{-u}u^{-i\tau-1}\,du,
\]

we have

\[
|\Gamma(-i\tau,\lambda)|\leq E_1(\lambda).
\]

Also,

\[
\frac1{|\Gamma(-i\tau)|}=|\tau|M_\tau.
\]

The required gamma modulus identity is given in DLMF §5.4 and was checked directly. [dlmf.nist.gov](https://dlmf.nist.gov/5.4)

Therefore

\[
\left|\mathbb E f_\tau(K)-\lambda^{i\tau}\right|
\leq |\tau|M_\tau E_1(\lambda).
\tag{12}
\]

Finally,

\[
f_\tau(k+1)
=
f_\tau(k)\frac{k+1}{k+1-i\tau},
\]

so \(|f_\tau(k)|\) is nonincreasing in \(k\), with maximum \(M_\tau\) at zero.

Conditional independence of the daily counts, followed by a telescoping product bound, now yields

\[
\left|
\mathbb E[U_{i,\gamma}\mid\mathscr F_H]
-
\exp\!\left(i\gamma\sum_d w_d\log\Lambda_{id}\right)
\right|
\leq
M_\gamma^{(L)}|\gamma|
\sum_d |w_d|E_1(\Lambda_{id}).
\tag{13}
\]

This step uses conditional count independence only. It makes no claim that thinning would produce independent latent state measurements.

### 3.2 Low intensity is controlled without a floor

Define

\[
\bar Y_{id}=\int_d^{d+1}Y_i(t)\,dt.
\]

Jensen’s inequality gives

\[
\Lambda_{id}\geq100e^{\bar Y_{id}}.
\tag{14}
\]

Under the null, \(\bar Y_{id}\) is Gaussian. A uniform upper bound on its variance is

\[
q_d=0.0015\left(d+\frac13\right)+0.09.
\tag{15}
\]

A uniform lower bound on its mean is

\[
m_d^-=
-1-0.001\left(d+\frac12\right)
-0.0003\bigl(|A_{1d}|+|A_{2d}|\bigr),
\tag{16}
\]

where the elementary calendar integrals are

\[
A_{1d}=\int_d^{d+1}\int_0^t\sin(2\pi u/365)\,du\,dt,
\]

\[
A_{2d}=\int_d^{d+1}\int_0^t\cos(2\pi u/365)\,du\,dt.
\]

For any \(p>0\),

\[
E_1(z)\leq p^{p-1}e^{-p}z^{-p}.
\tag{17}
\]

Indeed, \(e^{-u}\leq(p/e)^pu^{-p}\), and integrating \(u^{-p-1}\) proves the claim.

Combining (14)–(17),

\[
\mathbb E_\theta E_1(\Lambda_{id})
\leq
B_d,
\]

where the completely explicit bound used in the computation is

\[
B_d=
\min_{p\in\{1,\ldots,32\}}
p^{p-1}e^{-p}
100^{-p}
\exp\!\left[-pm_d^-+\frac{p^2q_d}{2}\right].
\tag{18}
\]

Thus the Poisson contribution at frequency \(\gamma\) is

\[
P_\gamma
=
M_\gamma^{(L)}|\gamma|
\sum_d |w_d|B_d.
\tag{19}
\]

No event excluding low-intensity paths has been introduced. The bound integrates their contribution, and all zero counts remain in the statistic.

### 3.3 Separate the daily Jensen correction

Define

\[
J_{id}
=
\log\int_d^{d+1}e^{Y_i(t)}\,dt-\bar Y_{id},
\qquad
\Delta_i=\sum_d w_dJ_{id}.
\]

Then \(J_{id}\geq0\), and exactly

\[
\sum_d w_d\log\Lambda_{id}=F_i+\Delta_i.
\tag{20}
\]

A direct absolute-error estimate using \(\sum|w_d|\mathbb EJ_{id}\) is unnecessarily large. Under the null, stationarity of the local Gaussian increments and Gaussian covariance identities give a much sharper bound on the **odd** error relevant to the test.

Use the constants

\[
B=0.001+\sqrt2(0.0003),
\qquad
D=\frac{2\pi}{365}\sqrt2(0.0003),
\]

\[
A=0.0015+2\frac{0.09}{7},
\qquad
e_0=\frac{B^2}{8}+\frac D4.
\tag{21}
\]

Here \(B\) bounds the absolute instantaneous deterministic drift and \(D\) bounds \(|s'(t)|\).

The following bounds hold uniformly under the null:

\[
|\mathbb E\Delta_i|\leq4e_0,
\tag{22}
\]

\[
\operatorname{Var}(\Delta_i)\leq
V_\Delta:=
\frac{3A}{2L}
\left[
\exp\!\left\{\frac A3+\frac{(2B+A/2)^2}{8}\right\}-1
\right],
\tag{23}
\]

and, writing

\[
r_L=0.0015+\frac{4(0.09)}L,
\tag{24}
\]

\[
\left|
\mathbb E\sin\{\gamma(F_i+\Delta_i-m)\}
\right|
\leq
J_\gamma,
\tag{25}
\]

with

\[
J_\gamma
=
4|\gamma|e_0
+\frac{|\gamma|^3r_L^2}{2}
+\frac{\gamma^2}{2}\left(V_\Delta+16e_0^2\right).
\tag{26}
\]

Here is the proof of these three claims.

#### Mean correction

On a day, subtract the daily average from the centered Gaussian part of \(Y_i\). Its law is the same on every day and is invariant under time reflection \(u\mapsto1-u\).

For the Brownian component, this follows by reversing the day’s increments and subtracting their average. For OU, it follows directly from its stationary covariance \(v_ie^{-\kappa_i|u-v|}\).

For a centered local Gaussian path \(G\), let

\[
j(\ell)=
\mathbb E\left[
\log\int_0^1 e^{G(u)+\ell(u-1/2)}\,du
\right].
\]

Reflection implies \(j(\ell)=j(-\ell)\), hence \(j'(0)=0\). The second derivative is a probability-weighted variance of \(u-1/2\), so

\[
0\leq j''(\ell)\leq\frac14.
\]

Therefore

\[
|j(\ell)-j(0)|\leq\frac{\ell^2}{8}.
\]

Approximating the within-day deterministic drift by its midpoint value produces a remainder with oscillation at most \(D/4\). The log-integral functional changes by at most this oscillation. Consequently,

\[
|\mathbb EJ_{id}-j_i(0)|\leq e_0.
\]

The constant \(j_i(0)\) may depend arbitrarily on the name’s \(\alpha_i,v_i,\kappa_i\), but \(\sum_dw_d=0\). Equation (22) follows.

#### Variance correction

For a zero-integral deterministic function \(f\), write \(K_f(t)=\int_t^Hf(u)\,du\). Integration by parts in the OU covariance gives

\[
\operatorname{Var}\!\left(\int f(t)Z_i(t)\,dt\right)
=
2\kappa_iv_i\int K_f(t)^2\,dt
-\kappa_i^2v_i
\iint K_f(s)K_f(t)e^{-\kappa_i|s-t|}\,ds\,dt.
\]

The last quadratic form is nonnegative. Hence the covariance of the Brownian-plus-OU process, when applied to zero-integral functions, is bounded above by

\[
(\alpha_i+2\kappa_iv_i)\int K_f(t)^2\,dt
\leq A\int K_f(t)^2\,dt.
\tag{27}
\]

For completeness, the Gaussian variance inequality used next follows from Gaussian interpolation:

\[
\operatorname{Var}\Phi(G)
=
\int_0^1
\mathbb E\langle D\Phi(G),C\,D\Phi(G_\rho)\rangle\,d\rho
\leq
\mathbb E\langle D\Phi(G),C\,D\Phi(G)\rangle.
\tag{28}
\]

Here \(G_\rho\) has the same marginal covariance \(C\), with cross-covariance \(\rho C\). The inequality is Cauchy–Schwarz. Applying finite-dimensional approximations and then taking limits gives the path version used here.

On a day, let

\[
p_d(u)=\frac{e^{Y_i(d+u)}}{\int_0^1e^{Y_i(d+t)}\,dt}.
\]

The functional derivative of \(J_{id}\) is \(p_d-1\), whose daily integral is zero. Moreover,

\[
\left|\int_0^u(p_d(t)-1)\,dt\right|^2
\leq
\frac14\int_0^1(p_d(t)-1)^2\,dt.
\tag{29}
\]

The variance of the daily-centered Gaussian path at position \(u\) is at most

\[
A\left(u^2-u+\frac13\right).
\]

Since the deterministic daily-centered mean has integral zero and oscillation at most \(B\), Jensen’s inequality and the bounded-range exponential inequality give

\[
\mathbb E\int_0^1p_d(u)^2\,du
\leq
\exp\!\left\{\frac A3+\frac{(2B+A/2)^2}{8}\right\}.
\tag{30}
\]

Equations (27)–(30), together with \(\sum_dw_d^2=6/L\), prove (23).

#### Odd covariance correction

Let \(G_i=F_i-m\), which is centered Gaussian under the null.

For any jointly Gaussian scalar \(G\) and Gaussian path \(V\), define \(g(u)=\operatorname{Cov}(G,V(u))\). A second Gaussian interpolation gives

\[
\left|
\operatorname{Cov}\!\left(\cos(\gamma G),J(V+\mu)\right)
\right|
\leq
\frac{\gamma^2}{8}\operatorname{osc}(g)^2.
\tag{31}
\]

To verify the constant, interpolate the cross-covariance from zero to \(g\). The first derivative at zero vanishes because \(\mathbb E\sin(\gamma G)=0\). The second derivative contains

\[
-\gamma^2\cos(\gamma G)\,D_g^2J.
\]

The second directional derivative \(D_g^2J\) is a probability-weighted variance of \(g\), bounded by \(\operatorname{osc}(g)^2/4\). Integrating the second derivative contributes the additional factor \(1/2\).

For the present filter, the Brownian contribution to the oscillation of \(g\) over one day is at most \(0.0015\). The OU contribution is at most

\[
2v_i\|w\|_\infty\leq\frac{4(0.09)}L.
\]

Thus \(\operatorname{osc}(g)\leq r_L\).

Taylor expansion with a bounded remainder gives

\[
\sin\{\gamma(G_i+\Delta_i)\}
=
\sin(\gamma G_i)
+\gamma\Delta_i\cos(\gamma G_i)
+R_i,
\qquad
|R_i|\leq\frac{\gamma^2\Delta_i^2}{2}.
\]

Apply (22), (23), and (31), sum the four units of absolute filter weight, and use

\[
\mathbb E\Delta_i^2\leq V_\Delta+16e_0^2.
\]

This proves (26).

### 3.4 Combine the two observation errors

From (13), (19), and (25),

\[
\left|
\mathbb E_\theta
\operatorname{Im}\!\left(e^{-i\gamma m}U_{i,\gamma}\right)
\right|
\leq
\epsilon_\gamma:=P_\gamma+J_\gamma
\qquad(\beta=0).
\tag{32}
\]

Therefore

\[
\epsilon_A=\epsilon_1,
\qquad
\epsilon_B=\epsilon_3+\frac32\epsilon_1,
\]

which proves the null expectation bounds in the theorem.

Notice where Gaussianity was used: **only under \(\beta=0\), where the latent contrast really is Gaussian.** No stochastic integral with state-dependent volatility has been treated as Gaussian conditional on its quadratic variation.

---

## 4. The actual confidence set and test

The preceding null test can be extended to a conservative confidence set over the full \(\beta\)-box.

### 4.1 Uniform score envelopes away from zero

For \(a=|\beta|\), put

\[
d(a)=\sqrt{0.001-a}+\sqrt{0.001}.
\]

Let

\[
K_A=M_1^{(L)},
\qquad
K_B=3M_3^{(L)}+\frac32M_1^{(L)},
\]

and define

\[
b_r(a)=
\min\left\{
R_r,\,
\epsilon_r+\frac{8K_ra\sqrt H}{d(a)}
\right\},
\qquad r\in\{A,B\}.
\tag{33}
\]

For the true seasonal phase,

\[
|\mathbb E_\theta S_{i,r}(m)|\leq b_r(|\beta|)
\tag{34}
\]

uniformly over the full parameter box.

Here is an observation-level proof rather than a latent endpoint substitution.

Couple \(X_i^\beta\) and \(X_i^0\) using the same \(W_i\), initial state, named drift, seasonal drift, and OU path. Their difference is a martingale, and

\[
\left|
\sqrt{\alpha_i+\beta\tanh X_i^\beta(t)}-\sqrt{\alpha_i}
\right|
\leq\frac{a}{d(a)}.
\]

Consequently, Doob’s inequality gives

\[
\mathbb E\sup_{t\leq H}|X_i^\beta(t)-X_i^0(t)|
\leq\frac{2a\sqrt H}{d(a)}.
\tag{35}
\]

For the **integrated** daily intensities,

\[
|\log\Lambda_{id}^\beta-\log\Lambda_{id}^0|
\leq
\sup_{d\leq t\leq d+1}|X_i^\beta(t)-X_i^0(t)|.
\tag{36}
\]

The conditional mean of \(f_\tau(K)\), as a function of \(\log\lambda\), has derivative bounded by \(|\tau|M_\tau\). Indeed,

\[
\lambda\left|\frac{d}{d\lambda}\mathbb Ef_\tau(K)\right|
\leq
|\tau|M_\tau
\lambda\mathbb E\frac1{K+1}
=
|\tau|M_\tau(1-e^{-\lambda}).
\]

The product conditional mean in (1) is therefore coordinatewise Lipschitz in the daily log intensities, with constants \(M_\gamma^{(L)}|\gamma w_d|\). Combining this fact with (35)–(36) proves (34).

This bound is intentionally conservative. Its purpose is valid inversion, not a claim of precise estimation.

### 4.2 Fixed betting factors

For \(r\in\{A,B\}\), sign \(\sigma\in\{-1,1\}\), and \(j=0,\ldots,7\), define the fixed stake

\[
\eta_{rj}(a)=
\frac{2^{-j}}{2(R_r+b_r(a))}
\]

and

\[
E_{r,\sigma,j}(\beta,m)
=
\prod_{i=1}^N
\left[
1+\eta_{rj}(|\beta|)
\{\sigma S_{i,r}(m)-b_r(|\beta|)\}
\right].
\tag{37}
\]

Every factor is at least \(1/2\). Form the predetermined mixture

\[
E(\beta,m)=
\frac1{32}
\sum_{r,\sigma,j}E_{r,\sigma,j}(\beta,m).
\tag{38}
\]

The confidence set is

\[
\boxed{
I(C)=
\left\{
\beta\in[-0.0005,0.0005]:
\min_{m\in\mathcal M}E(\beta,m)\leq20
\right\}.
}
\tag{39}
\]

The test rejects immutable variance types when

\[
\boxed{
\min_{m\in\mathcal M}E(0,m)>20.
}
\tag{40}
\]

Thus a seasonal value favorable to the null must also be rejected. The analyst is not allowed to choose the phase that maximizes apparent evidence.

### 4.3 Coverage proof

Fix the true parameter array and true phase \(m_0\). By (34), each factor in (37), evaluated at \((\beta,m_0)\), has expectation at most one.

The complete histories are independent across names. Hence

\[
\mathbb E_\theta E_{r,\sigma,j}(\beta,m_0)\leq1
\]

and therefore

\[
\mathbb E_\theta E(\beta,m_0)\leq1.
\]

If the true \(\beta\) is excluded from (39), then \(E(\beta,m_0)>20\). Markov’s inequality gives

\[
\Pr_\theta\{\beta\notin I(C)\}\leq .05.
\]

This proves (6) and (8). The same expectation-one inversion principle appears in finite-sample universal inference; the proof above is direct and does not require a likelihood approximation or a result about power from that literature. [arXiv](https://arxiv.org/html/1912.11436v4)

There is no additional multiplicity cost for the two scores, two signs, or eight stakes: they are averaged in the fixed mixture. Nor is there a separate seasonal-estimation failure event. Coverage is proved at the true phase, which lies inside the profiled interval.

### 4.4 What the confidence set can report

Because (33) depends on \(|\beta|\), this confidence set is symmetric in \(\beta\). It can certify a nonzero response magnitude, but **cannot distinguish positive from negative response**.

The observable rule

\[
Q(C)=\mathbf1\{0\notin I(C)\}
\]

has the joint guarantee

\[
\Pr_\theta\{
\beta\in I(C)
\ \text{and}\ 
[Q(C)=0\ \text{or}\ \beta\neq0]
\}\geq .95.
\tag{41}
\]

This is an evidence qualification rule, not a certified rule for adequate latent state coverage. I have not proved that it qualifies with probability .8 on the requested alternatives.

### 4.5 Computation and numerical decisions

The scores involve finitely many gamma evaluations and products. The seasonal minimization is one-dimensional on a compact interval.

For a rigorous implementation, evaluate the complex scores and the seasonal profile using outward interval arithmetic. Reject only after every phase interval has a lower bound exceeding 20. Unresolved numerical cells retain the candidate parameter.

The supplied reference implementation follows this rule. A computation limit can therefore cause failure to reject, but cannot invalidate coverage.

---

## 5. Quantitative budgets and executed calculations

### Observation-error budget

The following are outward-rounded upper bounds from executed interval calculations.

| Component | \(T=365\), \(L=121\) | \(T=180\), \(L=60\) |
|---|---:|---:|
| Poisson contribution \(P_1\) | 0.000265026 | 0.000010718 |
| Daily-integration odd contribution \(J_1\) | 0.000019875 | 0.000039555 |
| Poisson contribution \(P_3\) | 0.001101566 | 0.000061973 |
| Daily-integration odd contribution \(J_3\) | 0.000309211 | 0.000812333 |
| Combined null bias \(\epsilon_B\) | **0.001838127** | **0.000949713** |
| Score range \(R_A\) | 1.041624 | 1.085700 |
| Score range \(R_B\) | 3.005587 | 3.721143 |

The shorter history has a smaller low-intensity bound but a larger daily-integration term for the more concentrated filter. Its count-transform range is also larger.

### Finite-\(N\) uncertainty

The actual test uses (37)–(40), not a worst-range Hoeffding threshold. Nevertheless, the latter is a useful check against overstating precision.

For the two two-sided mean bounds at the true phase, a valid common Hoeffding multiplier is

\[
h_N=\sqrt{\frac{2\log80}{N}}.
\]

The combined-score radius would be \(\epsilon_B+R_Bh_N\):

| \(N\) | \(T\) | Whole-name observations | Conservative mean radius | Certified 80% power |
|---:|---:|---:|---:|---|
| 10,000 | 365 | 10,000 | 0.090816 | **Not established** |
| 10,000 | 180 | 10,000 | 0.111111 | **Not established** |
| 1,000 | 365 | 1,000 | 0.283211 | **Not established** |
| 1,000 | 180 | 1,000 | 0.349311 | **Not established** |

These radii demonstrate that a crude range-only argument is insufficient. **They do not establish low power of the betting test, much less statistical impossibility.**

### Failure-probability allocation

| Source | Treatment | Probability charged |
|---|---|---:|
| Fixed named types and starting-size association | Namewise null identity and uniform envelopes | 0 |
| Unknown OU parameters | Full-box analytic bounds | 0 |
| Named drift and seasonality | Exact linear cancellation, local remainder bound, phase profiling | 0 |
| Poisson error and low intensity | Integrated expectation bounds (18)–(19) | 0 |
| Within-day integration | Bounds (22)–(26) | 0 |
| Selection among scores, signs, and stakes | Predetermined mixture | 0 |
| Numerical approximation | Outward bounds; unresolved calculations retain | 0 |
| Whole-name sampling uncertainty | Expectation-one test and Markov inversion | **.05** |
| **Total** |  | **.05** |

A deterministic bias bound is not a failure event. Its cost appears in the score centering, where it reduces power.

### Reproducible code and execution status

volatility_response_code.zip[Complete code and numerical output](sandbox:/mnt/data/volatility_response_code.zip)

The archive contains the interval certificate, its executed output, and an interval reference implementation of the count-only test. The certificate can be reproduced with:

```bash
python certify_constants.py
```

Selected executed output:

```text
T=365, L=121, used_days=363
weighted_E1_null    upper 0.000254435253
variance_delta     upper 0.000003085851
bias_A             upper 0.000284900117
bias_B             upper 0.001838126193

T=180, L=60, used_days=180
weighted_E1_null    upper 0.000009871683
variance_delta     upper 0.000006223131
bias_A             upper 0.000050271910
bias_B             upper 0.000949712414
```

The displayed decimals are checked against interval upper endpoints before printing. The minimum in (18) is over 32 explicit integer choices; it does not depend on an uncertified continuous optimizer.

I also checked (11) by deterministic 70-digit Poisson summation at \(\lambda=0.01,1,10,100\) and \(\tau=0.02,0.2\). Residuals were below \(2\times10^{-71}\). Those checks are numerical verification, not the proof of the identity.

**Execution limitations:** no empirical dataset was analyzed, no stochastic simulation was run, and no alternative-region power calculation was certified. The test implementation received a deterministic software smoke check, but its full interval-profile runtime at 10,000 names has not been benchmarked.

---

## 6. The exact remaining power problem

The null theorem alone does not show that the state response creates a sufficiently large observable departure. There are two remaining substantive questions:

1. How far do the alternative count-score laws move away from every allowable null seasonal phase?
2. Is that movement large relative to the distribution of whole-name evidence scores?

Here is one explicit sufficient inequality that would complete a finite-sample power proof for this construction.

### A deterministic mean–variance certificate

Under the null calibration, let

\[
\ell_{i,r,\sigma,j}(m)
=
\log\left[
1+\eta_{rj}(0)
\{\sigma S_{i,r}(m)-\epsilon_r\}
\right].
\]

These log factors lie in

\[
[-\log2,\log(3/2)],
\]

an interval of length \(b=\log3\).

Take a deterministic phase grid with \(J\) points and maximum spacing \(h\), including both endpoints. Let

\[
L_{\mathrm{prof}}
=
N\max_{r\in\{A,B\}}
\frac{K_r}{R_r+\epsilon_r}.
\]

The logarithm of the mixture evidence is \(L_{\mathrm{prof}}\)-Lipschitz in phase. This follows by differentiating each positive factor and using its lower bound \(1/2\).

For a fixed alternative parameter array \(\theta\), suppose that at every grid point \(m_q\), at least one of the 32 predetermined components satisfies

\[
\sum_{i=1}^N
\mathbb E_\theta\ell_i(m_q)
-
\left[
\frac{bx}{3}
+
\sqrt{2V_qx+\left(\frac{bx}{3}\right)^2}
\right]
>
\log640+\frac{L_{\mathrm{prof}}h}{2},
\tag{42}
\]

where

\[
x=\log(J/.2),\qquad
V_q\geq\sum_i\operatorname{Var}_\theta\ell_i(m_q).
\]

Then rejection probability is at least .8.

To see this, the bounded-variable exponential-moment bound gives a lower-tail Bernstein inequality for each selected component. The bracket in (42) makes its failure probability at most \(.2/J\). A union bound covers the grid. The factor 32 in the mixture accounts for \(\log640=\log(20\cdot32)\), and the Lipschitz allowance extends rejection from the grid to the full phase interval.

The component used in this proof may depend on the fixed alternative array; it is not selected from the observed data. The test already contains all components.

**What has not been obtained is a rigorous enclosure of the expectations and variances in (42), uniformly over a materially sized alternative region.** Moreover, (42) is only a sufficient certificate. Its grid and concentration costs might themselves be too conservative; failure of (42) would not disprove useful power.

### The expectations must be computed under the actual count law

The appropriate daily operator is

\[
(K_{d,c}f)(x,z)
=
\mathbb E_{x,z}\left[
e^{-\Lambda_d}\frac{\Lambda_d^c}{c!}
f(X_{d+1},Z_{d+1})
\right],
\tag{43}
\]

with

\[
\Lambda_d=100\int_d^{d+1}e^{X_t+Z_t}\,dt.
\]

Products of these operators give expectations of whole-history count functions, including the log factors in (42). A deterministic certification could use these operators, justified truncation of counts and state space, and verified numerical enclosures.

I have not executed that certification. Replacing (43) by endpoint observations, known state labels, or a Gaussian approximation to the alternative stochastic integral would leave precisely the gap the question warns against.

### No impossibility conclusion follows

For any simple null/alternative pair of complete count laws \(P_0,P_1\), a level-.05 test obeys

\[
\mathbb E_{P_1}\phi
\leq .05+\operatorname{TV}(P_0,P_1).
\]

Thus a pair with total variation below .75 would obstruct 80% power for that alternative. A suitable least-favorable mixture could also establish an obstruction.

**No such comparison has been proved here.** Neither the conservative interval nor the uncompleted power calculation establishes impossibility.

---

## 7. Adverse checks

**Adverse type–size association.** The null proof holds separately for every name. High \(\alpha_i\) may be associated with any initial size, drift, or OU type. This cannot create the null odd mean being tested. It can, however, weaken alternative separation; the power proof cannot replace arbitrary arrays by a convenient mixing distribution.

**Slow persistent OU noise.** Both the daily covariance bound and the count-level calibration cover the entire \((v_i,\kappa_i)\) box. No OU parameter is estimated or set to zero. The treatment does not presume that an endpoint of the \(\kappa\)-interval always maximizes every finite-horizon variance.

**Low-count paths.** The transform is bounded at zero, and (18) pays for arbitrarily small integrated intensity in expectation. There is no conditioning on all counts being positive.

**Weak state variation.** The procedure does not assume a lower bound on observed or latent state coverage. Consequently, it also does not promise power when the state response produces little distinguishable movement over the analyzed era. That limitation remains in the missing alternative comparison, rather than being hidden in a denominator.

**Seasonality.** The annual phase radius is approximately .04378, not zero. Setting it to zero would be an appreciable uncharged assumption. The profile in (39)–(40) retains the entire allowed range.

---

## 8. The most focused additional calibration target

No added assumption has been proved necessary, and I cannot identify a weakest sufficient instrument without a power or lower-bound result.

For this construction, a useful, relatively focused calibration target is the **roster-average OU variance after applying the declared filter**, rather than every individual OU parameter.

For a name with parameters \(v,\kappa\),

\[
\operatorname{Var}\!\left(\int_0^{3L}w(t)Z(t)\,dt\right)
=
v\,F(\kappa L),
\]

where direct integration of the OU covariance gives

\[
F(q)=
\frac{
12(q-1+e^{-q})
-(8-2e^{-q})(1-e^{-q})^2
}{q^2}.
\tag{44}
\]

Thus the calibration target is

\[
\frac1N\sum_i v_iF(\kappa_iL)\leq v_*.
\tag{45}
\]

This does not require common OU rates, size-independent types, or individual parameter identification. It addresses measurement variance without changing the diffusion model.

For scale, at \(L=121,\ v=.09,\ \kappa=1/30\), the OU contribution in (44) is approximately .16004. The lasting Gaussian contribution under the null is \(\alpha L\), between .121 and .1815. Persistent transitory noise can therefore be comparable to the lasting component in this contrast.

A genuine independent-noise replica could help assess (45): a second simultaneous exposure channel sharing \(X_i\) but having an independent transitory process, rather than a Poisson split sharing the same \(Z_i\). With equal exposure, this is an additional channel-day for every analyzed name and day—not another supposedly free year of stable coefficients.

Such a calibration would still need an actual-count theorem and an explicit error allocation. For example, a 99% calibration statement would leave .04 for the final evidence test, changing its threshold from 20 to 25. **I have not supplied that calibration theorem or shown that (45) alone is sufficient for 80% power.** It improves the measurement problem; it does not guarantee informative state movement.

---

## 9. Scientific interpretation

A rejection by the proved test would support the following statement:

> Within the specified diffusion-plus-Gaussian-OU daily-count model, immutable named variance types alone cannot explain the observed count-law asymmetry.

That conclusion retains arbitrary association between permanent type and starting size. It is therefore different from observing that smaller endpoints have greater subsequent variance.

The present construction would **not** establish the sign of \(\beta\), a causal intervention effect of size, a platform mechanism, stationarity of lasting activity, or a stable rank-size distribution.

**Bottom line:** the count-only null calibration and uniform-coverage construction are proved, including a small explicit daily-integration correction. The requested practical distinction at \(\beta\leq-0.0004\)—with verified qualification and at least 80% power on a nontrivial nuisance region—remains **UNRESOLVED**.

### Sources checked

National Institute of Standards and Technology. (n.d.). *Digital Library of Mathematical Functions*: [§5.4, Special values and extrema](https://dlmf.nist.gov/5.4), and [§8.7, Series expansions](https://dlmf.nist.gov/8.7).

Wasserman, L., Ramdas, A., & Balakrishnan, S. (2019). *Universal inference* [Preprint]. arXiv. [Paper](https://arxiv.org/abs/1912.11436).

The numerical certificate and model-specific observation bounds above were derived and checked in this response; no power claim was taken from these sources.
