# The estimand bridge: named movement, instantaneous variance, and stationary occupancy

**Date:** 2026-10-12 (Japan time). **Status:** THEORETICAL RESEARCH; no change adopted. Complete counterexamples and restricted identification results are given below; their applicability to the project is not established. No novelty or independent-verification claim is made.

**Provenance:** OpenAI Codex, GPT-6; starting commit `2602abd30e638b505d30a5d221c90f9e39939559`, branch `codex/d1-debate`, working copy `/Users/hindman/Documents/GitHub/rank-diffusion-codex-debate`. This responds to `PROOF_1_ESTIMAND_BRIDGE.md`. `variance_structure/THEORY.md` was used as a source map, not as proof authority. Project instructions, the stochastic-modeling skill and its evidence-standards reference, record-authoring and repository-maintenance skills, coordination protocol, PROGRAM_STATE, and relevant MODEL_STATUS sections (§2z-aw, §2z-ay, §2z-az, §2z-ba) were read. No empirical result is used as a premise.

## 1. Answer and scope

The general identification claim is false. Even the entire infinite weekly panel law, with correct names and permanent labels, does not identify the instantaneous variance in the class of stationary continuous Itô processes allowed by the prompt. There are two separate obstructions:

1. **Signal/noise allocation.** Independent but unknown serially correlated observation noise can exchange roles with a latent diffusion, leaving every observable unchanged (Claim 2).
2. **Continuous-time aliasing.** Even error-free weekly observations can conceal a stationary hidden oscillation that changes quadratic variation while preserving the entire sampled process law and the stationary ladder (Claim 3).

Neither obstruction is merely a failure of the two-lag statistic. Increasing the number of endpoints or the number of weeks cannot distinguish the respective constructions.

There are useful positive results, without identifying coefficients by fitting the stationary ladder:

- With identified observation error and observed persistent types, a stationary scalar **reversible Markov diffusion within each type** has its generator, variance function, and occupancy-weighted variance identified from the law at one positive lag (Claim 7). The operator proof is the classical low-frequency spectral identification argument, combined here with explicit noise removal and occupancy transport.
- Stationary typed OU dynamics with independent, temporally uncorrelated, mean-zero observation errors admit elementary exact formulas from three weekly observations per endpoint at the population-law level, even with unknown error variance (Claim 8).
- Without a Markov restriction, noise-managed observations at lags tending to zero identify the occupancy-band target under stated moment continuity conditions (Claim 9). This is an additional observation experiment, not a conclusion about a fixed weekly panel.
- At finite lags, Claim 5 gives an explicit deterministic bias enclosure, retaining drift–martingale covariance. Claim 10 separates coefficient error, occupancy error, and within-type state dependence. Claim 11 states exactly what these errors imply for the ladder.

Identification here means uniqueness from an exact observable probability law within a declared class. It is not stable inversion, consistent estimation, finite-sample precision, or causal identification. These distinctions are used throughout.

## 2. Experiment, conditioning, and the target

### 2.1 Baseline experiment

Fix a population of N named endpoints, no births, deaths, censoring, or jumps. Deterministic seasonality is assumed known and removed. Observations are **point observations**, not weekly integrals or averages. These are substantive simplifications.

On a filtered probability space satisfying the usual conditions, let

\[
 X_i(t)=X_i(0)+\int_0^t\beta_i(s)\,ds+M_i(t),\qquad
 d\langle M_i,M_j\rangle_t=c_{ij}(t)\,dt,\qquad a_i=c_{ii}.
 \tag{1}
\]

The martingales are continuous and square integrable on every finite interval used below. Drifts are progressively measurable with finite integrated second moments. Covariances are predictable and positive semidefinite; their diagonals are integrable. These conditions suffice for the conditional martingale means and isometries used below. Where stronger conditions are needed they are stated locally. Cross-endpoint independence is not assumed in (1).

A declared calibration sigma-field \(\mathcal H\subseteq\mathcal F_0\) supplies persistent labels \(C_i\). Labels are fixed before the evaluation window. Martingale assumptions are with respect to a filtration that already contains \(\mathcal H\). Merely declaring information to be known does not preserve the martingale property if it contains future information.

We observe

\[
 Y_{i,n}=X_i(n\Delta)+e_{i,n},\quad n=0,\ldots,m,
 \quad T=m\Delta,\quad \Delta>0. \tag{2}
\]

Unless a claim says otherwise, no independence, centering, stationarity, or known covariance is imposed on e. The observable law includes names and calibration, not just unlabeled snapshots. A parameter is identified if any two admissible models with the same joint law of \((\mathcal H,\{Y_{i,n}\})\) give it the same value.

For the increment functional, sample \((I,t)\) using a predeclared probability law on endpoint/start-time pairs, independent of future innovations. Use the **same** law at both lags, with starts no later than \(T-h_2\). Equal weights over endpoints and then eligible starts are one possible choice. We condition on \(C_I=C\) and use one pooled conditional mean for each h:

\[
 D_C(h)=\operatorname{Var}(Y_I(t+h)-Y_I(t)\mid C_I=C),\qquad
 A_C=\frac{D_C(h_2)-D_C(h_1)}{h_2-h_1}. \tag{3}
\]

All conditional expectations below average over the specified pair distribution as well as process randomness. They can instead be conditional on a realized \(\mathcal H\), provided **all** targets and assumptions are interpreted under that same conditional law. Our stationary identification theorems condition on an observed exogenous type, not automatically on the entire realized calibration history. A separated calibration window does not make its labels independent of future states, nor does it preserve stationarity after selection. Conditioning on a past high state of an OU process, for example, changes future conditional means.

An endpoint-specific centering of its increments changes (3): by total variance it removes the between-endpoint variance of their population mean increments. Centering by *sample* endpoint means removes additional random variation; §8 gives an exact example.

### 2.2 Relative coordinates are part of the parameter

If absolute coordinates Z have instantaneous covariance matrix q and the prescribed relative coordinates are \(X_i=Z_i-\sum_jw_jZ_j\), with known constant weights summing to one, then

\[
 a_i^{\rm rel}=q_{ii}-2\sum_jw_jq_{ij}+\sum_{j,k}w_jw_kq_{jk}. \tag{4}
\]

This follows by expanding the bracket of the linear combination. Relative drift transforms by the same linear map. A shared Brownian shock can disappear under centering; a shock with unequal loadings generally does not. A deterministic external reference is also an admissible declared coordinate.

If a center is estimated, write the residual centering error as r and incorporate \(e_i-r\) in (2). It will generally be correlated with latent increments and across names. Time-varying random weights also generate product-rule terms. No theorem below treats an estimated center as known. Ordinal-rank variance is a different quantity from variance of relative log activity.

### 2.3 Occupancy and stationary balance

Suppose the uniformly sampled endpoint has a stationary joint marginal of \((X_I,\beta_I,a_I)\), with X-density p positive on the interval considered. Define

\[
 b_*(x)=\mathbb E[\beta_I(t)\mid X_I(t)=x],\quad
 a_*(x)=\mathbb E[a_I(t)\mid X_I(t)=x]. \tag{5}
\]

Conditional expectations are defined p-almost everywhere. Bracket densities are intrinsically defined only up to dt⊗P-null sets; fixed-time statements use the stationary versions stipulated here, or the right-mean continuity in (27), rather than arbitrary redefinitions at sampling dates. Pointwise-in-x conclusions require a continuous version or other regularity. Write \(u(x)=\int_x^\infty p(y)dy\), descending quantile \(x(u)\), and \(S(u)=u/p(x(u))\). For \(B=[u_1,u_2]\subset(0,1)\), set \(m_B=u_2-u_1\) and \(J_B=[x(u_2),x(u_1)]\). Then

\[
 a_B=\frac1{m_B}\int_{J_B}a_*(x)p(x)dx
     =\frac1{m_B}\int_{u_1}^{u_2}a_*(x(v))dv. \tag{6}
\]

Endpoints in this band are selected by **contemporaneous latent occupancy**, not permanent rank and not observed noisy current rank.

**Claim 1 (why occupancy is the structural coefficient).** Under (1), stationarity, and integrability sufficient for Itô's formula in expectation for each \(f\in C_c^2(\mathbb R)\),

\[
 0=\int\left(b_*f'+\tfrac12a_*f''\right)p\,dx. \tag{7}
\]

If \(b_*p\) is locally integrable and \(a_*p\) has the required weak derivatives, the current \(J=b_*p-(a_*p)'/2\) is constant in x in the distributional sense. If a no-flux condition makes that constant zero, \(a_*p\to0\) at the upper boundary, and the upper-tail drift integral exists, then

\[
 a_*(x)p(x)=-2\int_x^\infty b_*(y)p(y)dy=2G(u(x)),
 \qquad S(u)=\frac{u\,a_*(x(u))}{2G(u)}. \tag{8}
\]

**Proof.** Apply Itô's formula to each name, take expectations, average uniformly over names, and condition its drift and bracket terms on the current X. Stationarity makes the expected time derivative zero. Integration by parts against compactly supported tests gives \(-J'=0\); stationarity alone does not select the constant. Under the stated boundary conditions, integrate \((a_*p)'=2b_*p\) from x upward. Finally use \(p(x(u))=u/S(u)\). ∎

This proof does not require the scalar named process to be Markov. Consequently it does **not** justify replacing that process's transition law by a diffusion with coefficients (5). The distinction is central to Claim 3. Conditional projection/mimicking prior art [R3] supports the one-time-marginal distinction; no mimicking theorem is needed for this proof. The rank-quantile formula is known background [R4], not this note's contribution. Stationarity with entry/exit, jumps, a nonzero current, or an unaccounted boundary term requires a different balance equation.

## 3. Nonidentification from the entire observation law

### 3.1 Unknown serial noise, even independent of the signal

**Claim 2.** For every \(\Delta>0\), every finite N and every observation span, the model class in (1)–(2) with stationary independent signal and stationary unknown serial observation noise does not identify \(a_B\). The failure persists for observation at all lattice times, and even for continuous observation of their sum.

**Construction and proof.** Choose \(v>0\), \(\kappa_1\ne\kappa_2>0\). For each name take independent stationary OU processes

\[
 dU_i=-\kappa_1U_i\,dt+\sqrt{2\kappa_1v}\,dW_i^1,
 \qquad dV_i=-\kappa_2V_i\,dt+\sqrt{2\kappa_2v}\,dW_i^2.
\]

Different names' pairs can be independent. Fix permanent means \(\mu_i\), their labels, and any external calibration information identically in both models. One may supply the true means themselves as calibration, so uncertainty about permanent rank is not necessary for the counterexample.

- Model A: \(X_i=\mu_i+U_i\), \(e_{i,n}=V_i(n\Delta)\).
- Model B: \(X_i=\mu_i+V_i\), \(e_{i,n}=U_i(n\Delta)\).

In this coupling, Y is identically \(\mu_i+U_i+V_i\) at every observation, path by path. Both errors are mean zero, stationary, independent of the full corresponding latent process, and serially correlated. The latent stationary density is the **same** mixture of \(N(\mu_i,v)\) in both models. Their constant named variances are respectively \(2\kappa_1v\) and \(2\kappa_2v\), hence so are \(a_*\) and every \(a_B\). All joint calibration/observation laws agree. Both models are nonexplosive stationary continuous diffusions with all the needed finite moments. ∎

This is stronger than allowing an adversarial error \(e=-X\). It rules out using independence from X alone as the identifying noise assumption. Knowing only a contemporaneous noise variance does not repair this construction: both errors have variance v. A noise covariance law, an actual independent measurement instrument, or a stronger dynamic noise restriction is needed.

### 3.2 No noise: a stationary hidden-state alias

**Claim 3.** For every \(\Delta>0\) and \(\kappa>0\), there are two stationary continuous Gaussian Itô models with **zero observation error**, the same full lattice-time panel and calibration law and the same stationary density, but \(a_B=2\kappa\) in one and \(a_B=\kappa\) in the other, for every interior band B.

**Construction.** Put

\[
 \omega=\frac{2\pi}{\Delta},\quad d=\frac{\kappa}{2\omega},\quad
 s=\sqrt{1+d^2},\quad
 K=\begin{pmatrix}d&-s\\s&-d\end{pmatrix},\quad K^2=-I_2.
\]

For each name, use a two-dimensional latent state \(Z_i=(Z_{i,1},Z_{i,2})^\top\), with independent copies over names, and report \(X_i=\mu_i+Z_{i,1}\). The second coordinate is an unobserved state of that endpoint, not a second endpoint in the occupancy average. Compare

\[
 A_0=-\kappa I_2,\quad Q_0=2\kappa I_2,
 \qquad A_1=-\kappa I_2+\omega K,\quad
 Q_1=\begin{pmatrix}\kappa&0\\0&3\kappa\end{pmatrix},
 \qquad dZ_i=A_jZ_i\,dt+Q_j^{1/2}dW_i. \tag{9}
\]

Initialize each process with \(N(0,I_2)\), independently of future Brownian increments.

**Proof of stationarity and admissibility.** Directly,

\[
 A_j+A_j^\top+Q_j=0.
\]

The eigenvalues of \(A_1\) are \(-\kappa\pm i\omega\); those of \(A_0\) are \(-\kappa\). Both are stable and Q is positive definite. The explicit Gaussian solution has covariance

\[
 e^{A_jt}e^{A_j^\top t}+\int_0^t e^{A_jr}Q_je^{A_j^\top r}dr=I_2,
\]

as follows by differentiating \(e^{A_jr}e^{A_j^\top r}\). Thus the Gaussian initial law is invariant and the time-homogeneous process is stationary. Its linear coefficients give a global continuous solution and all finite moments required in (1). The first coordinate has adapted drift depending on the second and constant bracket density \((Q_j)_{11}\).

**Proof of equality of all observations.** Because \(K^2=-I_2\),

\[
 e^{A_1\Delta}=e^{-\kappa\Delta}
       (I_2\cos(2\pi)+K\sin(2\pi))=rI_2=e^{A_0\Delta},
 \qquad r=e^{-\kappa\Delta}.
\]

The covariance of the transition innovation in either model is
\(I_2-e^{A_j\Delta}e^{A_j^\top\Delta}=(1-r^2)I_2\).
Consequently their entire sampled vector chains have the same initial law and the same transition kernel
\(N(rz,(1-r^2)I_2)\). This proves equality of every finite-dimensional lattice law, not merely two covariances. Projecting to the first coordinate and taking independent copies proves equality of the full named panel law for every N and m. Fixed calibrated means/labels have the same law; past lattice observations used for calibration can also be included.

Both latent X-marginals are the same mixture of \(N(\mu_i,1)\). Nevertheless \(a_i\equiv2\kappa\) in model 0 and \(a_i\equiv\kappa\) in model 1. Therefore (6) differs exactly as asserted. ∎

**Audit against stationary balance.** For one mean-zero type, model 1 has
\(\mathbb E[Z_2\mid Z_1=x]=0\), so
\(b_*(x)=(-\kappa+\omega d)x=-\kappa x/2\),
whereas model 0 has \(b_*(x)=-\kappa x\). In both cases
\(b_*=-a_*x/2\), which gives zero current for the standard normal density and a vanishing upper-boundary product. Both therefore satisfy the structural equation exactly. The ratio of variance to restoring drift agrees while their absolute clocks differ.

The model-1 covariance at an arbitrary positive lag is

\[
 \mathbb E[Z_1(t+h)Z_1(t)]
 =e^{-\kappa h}\{\cos(\omega h)+d\sin(\omega h)\}. \tag{10}
\]

It is an ordinary OU covariance on the lattice, but is not an OU covariance between observations. The hidden model has stationary drift second moment \(\mathbb E[\beta^2]=\omega^2+\kappa^2/2\): square its two drift coefficients and use the independent unit-variance coordinates. Thus an independently justified small bound on subweekly drift could exclude this particular alias; no such restriction is supplied in the general experiment. Applying a scalar OU fit or a reversible scalar generator logarithm to this lattice law would return model 0's variance even when model 1 generated it. The projected coefficients in (5) give a process with the same stationary **marginal**, not model 1's sampled dynamics.

This construction permits deterministic external centering. If exact population-mean centering is required, subtract the sample mean of the N independent first coordinates in both models, N≥2. Their sampled laws still agree, their stationary marginal variances both become \(1-1/N\), and their relative instantaneous variances become respectively \(2\kappa(1-1/N)\) and \(\kappa(1-1/N)\). Thus centering does not rescue identification.

Adding the same independent, known noise to both models, or arbitrarily many independent measurement replicates **on the same lattice**, also leaves their observable laws equal. Finite grids at commensurable frequencies have the same obstruction, using their common finer grid. The construction does not prove failure for every irregular observation design; nor does merely adding one off-grid lag prove identification in the general Itô class.

## 4. What the finite-lag statistic actually measures

### 4.1 Exact decomposition

**Claim 4.** Under §2.1 and a common endpoint/start distribution, define, conditionally on cohort C,

\[
 B_h=\int_t^{t+h}\beta_I(s)ds,\quad Z_h=M_I(t+h)-M_I(t),
 \quad E_h=e_I(t+h)-e_I(t),
\]

\[
 Q_C(h)=\mathbb E\!\left[\int_t^{t+h}a_I(s)ds\mid C\right],\quad
 N_C(h)=\operatorname{Var}(E_h\mid C),
 \quad H_C(h)=2\operatorname{Cov}(B_h+Z_h,E_h\mid C).
\]

Then exactly

\[
 D_C(h)=Q_C(h)+\operatorname{Var}(B_h\mid C)
       +2\operatorname{Cov}(B_h,Z_h\mid C)+N_C(h)+H_C(h). \tag{11}
\]

**Proof.** Condition first on each selected name, start, and information available at that start. Martingale increments have mean zero; the conditional isometry gives \(\mathbb E[Z_h^2\mid C]=Q_C(h)\). Averaging over the nonanticipating pair distribution preserves these statements. Expand the conditional variance of \(B_h+Z_h+E_h\). ∎

Future drift is adapted, not known at the start; its covariance with \(Z_h\) generally does **not** vanish. Independence of the entire error vector and latent path, conditional on the selection information, makes H zero. Unconditional independence can be lost after conditioning on a label estimated using those errors.

If error increments have zero conditional mean and their covariance is stationary under this same pair/cohort law, with \(R_C(h)=\operatorname{Cov}(e_I(t+h),e_I(t)\mid C)\), then

\[
 N_C(h)=2\{R_C(0)-R_C(h)\}. \tag{12}
\]

Averaging endpoint-specific stationary covariance functions also gives (12) when the conditional increment means vanish. Serially dependent noise does not necessarily have a variance plateau at either chosen lag.

If the average bracket density under the cohort law is constant over the forward interval, \(Q_C(h)=h\alpha_C\), with \(\alpha_C=\mathbb E[a_I(t)\mid C]\). Writing
\(V_C(h)=\operatorname{Var}(B_h\mid C)+2\operatorname{Cov}(B_h,Z_h\mid C)\),

\[
 A_C=\alpha_C+
 \frac{V_C(h_2)-V_C(h_1)+N_C(h_2)-N_C(h_1)+H_C(h_2)-H_C(h_1)}{h_2-h_1}.
 \tag{13}
\]

This is a cohort bracket average plus finite-horizon corrections, still not an occupancy coefficient. Without the constant-average condition, even the Q term is a difference of integrated forward variances under selected starts.

### 4.2 A finite-lag error enclosure with all cross terms retained

**Claim 5.** Fix a selection event C available at the start; it may be a permanent cohort or the **oracle** event \(X_I(t)\in J_B\). Suppose for \(0\le s\le h_2\), under its pair law,

\[
 \mathbb E[\beta_I(t+s)^2\mid C]\le K_\beta^2,\qquad
 \mathbb E[a_I(t+s)\mid C]\le\bar a,
\]

and put

\[
 \alpha_C=\mathbb E[a_I(t)\mid C],\qquad
 \eta_C(h)=\frac1h\int_0^h
 \left|\mathbb E[a_I(t+s)-a_I(t)\mid C]\right|ds.
\]

Let \(\widetilde N_C(h)\) be an independently supplied correction and suppose
\(|N_C(h)-\widetilde N_C(h)|\le\delta_N(h)\) and
\(|H_C(h)|\le\delta_H(h)\). Define

\[
 F_C(h)=h\eta_C(h)+K_\beta^2h^2+
       2K_\beta\sqrt{\bar a}\,h^{3/2}+\delta_N(h)+\delta_H(h).
\]

Then

\[
 \left|\frac{D_C(h)-\widetilde N_C(h)}h-\alpha_C\right|
 \le\frac{F_C(h)}h, \tag{14}
\]

\[
 \left|\frac{[D_C(h_2)-\widetilde N_C(h_2)]-[D_C(h_1)-\widetilde N_C(h_1)]}
 {h_2-h_1}-\alpha_C\right|
 \le\frac{F_C(h_2)+F_C(h_1)}{h_2-h_1}. \tag{15}
\]

**Proof.** Jensen/Cauchy–Schwarz yields
\(\operatorname{Var}(B_h\mid C)\le\mathbb E[B_h^2\mid C]\le K_\beta^2h^2\).
The isometry gives \(\operatorname{Var}(Z_h\mid C)\le\bar a h\). Therefore
\(2|\operatorname{Cov}(B_h,Z_h\mid C)|\le2K_\beta\sqrt{\bar a}h^{3/2}\).
Fubini gives \(|Q_C(h)-h\alpha_C|\le h\eta_C(h)\).
Insert these and the two supplied error bounds in (11). Subtract the two resulting approximations \(h_j\alpha_C\) and apply the triangle inequality to obtain (15). ∎

The right side is an explicit **outer enclosure** of the compatible values, not a proved sharp identified set or a confidence interval. For oracle occupancy, \(\alpha_C=a_B\). For permanent cohorts an additional transport step is essential. Nonnegative variance also permits intersection with \([0,\infty)\).

No useful numerical guarantee follows unless the constants, conditional noise correction, and state/variance variation bounds have an independent justification. In particular, increasing N reduces sampling error but does not reduce this fixed-horizon bound. Closely spaced long lags can make (15) especially poorly conditioned.

**Refinement.** If additionally \(\beta_I(t)\) is start-measurable and
\(\|\beta_I(t+s)-\beta_I(t)\|_{2,C}\le L_\beta s^\gamma\), then
\(\operatorname{Cov}(h\beta_I(t),Z_h\mid C)=0\). Minkowski's inequality bounds the norm of the remaining integrated drift by
\(L_\beta h^{1+\gamma}/(1+\gamma)\). Thus the cross-term bound in F may be replaced by
\(2L_\beta\sqrt{\bar a}\,h^{3/2+\gamma}/(1+\gamma)\).
For \(\gamma=1/2\) it is order \(h^2\), hence order h in (14). This improvement requires the extra conditional regularity; it is not free from stationarity.

### 4.3 Three elementary audits

**Audit A: constant drift Brownian motion.** Let \(X(t)=x_0+\mu t+\sqrt aW(t)\), with deterministic \(\mu,a\), and independent stationary noise of covariance R. Then

\[
 D(h)=ah+2\{R(0)-R(h)\},\qquad
 A=a+\frac{2\{R(h_1)-R(h_2)\}}{h_2-h_1}. \tag{16}
\]

The deterministic drift is removed by the pooled mean. Independent errors with zero covariance at both positive lags cancel exactly. Brownian motion with drift is not stationary on the line; this audits the instrument, not the ladder.

**Audit B: heterogeneous endpoint trends.** Let
\(X_i(t)=x_{i0}+\mu_it+\sqrt{a_i}W_i(t)\), with \((\mu_i,a_i)\) known at time zero and independent of future Brownian innovations. Within a cohort they may be correlated with each other. With independent stationary error and common pairs,

\[
 D_C(h)=h\mathbb E[a_i\mid C]+h^2\operatorname{Var}(\mu_i\mid C)
             +2\{R_C(0)-R_C(h)\},
\]

\[
 A_C=\mathbb E[a_i\mid C]+(h_1+h_2)\operatorname{Var}(\mu_i\mid C)
       +\frac{2\{R_C(h_1)-R_C(h_2)\}}{h_2-h_1}. \tag{17}
\]

Proof: condition on endpoint characteristics, apply the martingale isometry and then total variance. Subtracting each endpoint's *true* drift removes the second term. Estimating that drift from the same short trajectory does not do so without further error (§8).

**Audit C: stationary OU.** Let \(dX=-\kappa(X-\mu)dt+\sqrt a\,dW\), \(\kappa>0\), initialized at stationarity with \(v=a/(2\kappa)\). The explicit solution gives covariance \(ve^{-\kappa h}\), so

\[
 D(h)=\frac a\kappa(1-e^{-\kappa h})+2\{R(0)-R(h)\}. \tag{18}
\]

For noise with equal covariance at the two positive lags,

\[
 A= a\,\frac{e^{-\kappa h_1}-e^{-\kappa h_2}}
                 {\kappa(h_2-h_1)}
   =\frac a{h_2-h_1}\int_{h_1}^{h_2}e^{-\kappa r}dr < a. \tag{19}
\]

It approaches a only when both horizons tend to zero (with their ratio, for example, fixed). Taking longer horizons at fixed weekly frequency drives this quantity downward, not toward the instantaneous variance.

For an explicit check of the disputed cross term, with \(B_h=-\kappa\int_0^h(X_s-\mu)ds\) and \(Z_h=\sqrt a(W_h-W_0)\), direct integration gives

\[
 \operatorname{Var}(B_h)=a\left[h-\frac{1-e^{-\kappa h}}\kappa\right],\qquad
 \operatorname{Cov}(B_h,Z_h)=-a\left[h-\frac{1-e^{-\kappa h}}\kappa\right]. \tag{20}
\]

For the first identity integrate \(\kappa^2ve^{-\kappa|r-s|}\) over the square; for the second integrate
\(-\kappa a\int_0^h\int_0^s e^{-\kappa(s-r)}drds\).
Their substitution into (11) gives (18). Deleting the cross term would give the wrong sign of the finite-lag effect.

## 5. Positive identification at fixed frequency

### 5.1 Observation-law recovery

**Claim 6 (two sufficient noise instruments).** Let Z be a finite-dimensional latent vector, for example \((X_I(0),X_I(\Delta))\) conditional on a type.

**(a) Known joint error law.** If \(Y=Z+E\), E is independent of Z, and its known characteristic function \(\phi_E(t)\) is nowhere zero, then the law of Z is identified:

\[
 \phi_Z(t)=\phi_Y(t)/\phi_E(t). \tag{21}
\]

Serial dependence *inside* E is allowed; it is the joint vector law, not just the marginal variance, that is needed. Independence must hold under the conditioning used.

**(b) Independent repeated measurement vectors.** Suppose two channels report
\(Y^{(1)}=Z+E^{(1)}\), \(Y^{(2)}=Z+E^{(2)}\), with Z and the two error vectors mutually independent, finite first absolute moments, \(\mathbb E E^{(1)}=0\), and \(\phi_Z\) and \(\phi_{E^{(2)}}\) nowhere zero. Then Z and both error-vector laws are identified. Channel errors may each be serially correlated; the two entire error vectors must be independent.

**Proof.** Part (a) is multiplication of characteristic functions and uniqueness of their associated probability laws. For (b), let

\[
 \Psi(s,t)=\mathbb E e^{i(s^\top Y^{(1)}+t^\top Y^{(2)})}
          =\phi_Z(s+t)\phi_{E^{(1)}}(s)\phi_{E^{(2)}}(t).
\]

First moments justify differentiating in s. The zero mean and the nonvanishing denominator give

\[
 R(t):=\frac{\nabla_s\Psi(0,t)}{\Psi(0,t)}
       =\frac{\nabla\phi_Z(t)}{\phi_Z(t)},\qquad
 \phi_Z(t)=\exp\left(\int_0^1 t^\top R(rt)dr\right). \tag{22}
\]

The last equality follows by solving the scalar differential equation for \(r\mapsto\phi_Z(rt)\), with value one at r=0; it requires no choice of a complex logarithm branch. The marginal characteristic functions of each Y then identify each error law by division by \(\phi_Z\). ∎

This is a finite-vector version of the classical repeated-measurement argument, not a new principle; Kotlarski's original Lemma 1 and proof were checked [R2]. The assumptions here are sufficient, not necessary. The refinements in [R5] are not imported into (22). Only the second channel's error characteristic function is required nonvanishing in this proof. Two errors at the same timestamp are not automatically independent instruments, and two different days do not measure the same latent Z. A replicate difference alone need not identify an asymmetric error law. Shared calibration or centering errors can also destroy the factorization.

### 5.2 A nonparametric restricted theorem

**Claim 7 (identified reversible typed diffusions at any fixed positive lag).** Fix any \(\Delta>0\). Consider the following class \(\mathcal R\):

1. A uniformly sampled endpoint has an observed persistent type \(\theta\in\{1,\ldots,K\}\), with \(\pi_\theta>0\). A type can be a permanent cohort only if the following within-type restrictions actually hold. Types are supplied exogenously or by calibration whose joint law is included and whose conditioning preserves these restrictions.
2. Conditional on type, the **scalar latent coordinate itself** is a stationary, conservative, time-homogeneous Markov diffusion on \(\mathbb R\), with a strictly positive invariant density \(p_\theta\), continuous coefficients \(a_\theta>0,b_\theta\), \(a_i(t)=a_\theta(X_i(t))\), \(\beta_i(t)=b_\theta(X_i(t))\), and
\[
 L_\theta f=b_\theta f'+\tfrac12a_\theta f'',\qquad f\in C_c^\infty(\mathbb R).
\]
It has a strongly continuous semigroup on \(L^2(p_\theta)\). Its generator is **self-adjoint and nonpositive**, and its domain contains these tests and their squares. Local boundedness ensures the displayed test images lie in \(L^2(p_\theta)\). These are explicit reversibility and domain assumptions, not consequences of a fit to weekly data.
3. The conditional noisy two-time law identifies the latent two-time law by Claim 6(a) or (b). Known noiseless measurement is included.
4. The mixture has finite bracket moments for the target, and its density is positive over the band used.

Then the joint observation/type law at times 0 and \(\Delta\) identifies each \(p_\theta\), \(L_\theta\), \(a_\theta,b_\theta\) (initially almost everywhere), and therefore

\[
 p(x)=\sum_\theta\pi_\theta p_\theta(x),\qquad
 a_*(x)=\frac{\sum_\theta\pi_\theta p_\theta(x)a_\theta(x)}{p(x)},
\]

\[
 a_B=\frac1{m_B}\sum_\theta\pi_\theta
             \int_{J_B}a_\theta(x)p_\theta(x)dx. \tag{23}
\]

The same expression with b identifies the effective drift; no stationary ladder equation is used in this identification.

**Proof, step 1: recover an operator.** Deconvolution gives the conditional joint measure
\(\nu_\theta(dx,dy)=p_\theta(x)dx\,P_{\theta,\Delta}(x,dy)\).
Its first marginal identifies p and disintegration identifies the kernel p-almost everywhere. Consequently the bounded contraction operator \(P_{\theta,\Delta}\) on the now-known \(L^2(p_\theta)\) is identified. Two candidate models producing this same measure act on the same Hilbert space and have the same operator there.

**Step 2: uniqueness of the generator.** For clarity, the standard self-adjoint spectral-calculus fact used here has this exact form: for a self-adjoint operator L with spectrum in \(( -\infty,0]\), its spectral measure E gives
\(e^{\Delta L}=\int e^{\Delta\lambda}E(d\lambda)\).
This is a positive contraction with zero kernel, although zero may be a limit point of its spectrum. If F is its spectral measure, the operator

\[
 \Delta^{-1}\log P=\Delta^{-1}\int_{(0,1]}\log r\,F(dr),\qquad
 \mathcal D(\log P)=\left\{f:\int_{(0,1]}|\log r|^2d\langle F(r)f,f\rangle<\infty\right\}
 \tag{24}
\]

is well defined and equals L, including its domain. Indeed the scalar map \(\lambda\mapsto e^{\Delta\lambda}\) is one-to-one on the real half-line, and substitution in the spectral integral gives \(\log(e^{\Delta\lambda})=\Delta\lambda\) and the identical square-integrability condition. Zero has no spectral atom because \(e^{\Delta\lambda}>0\) for every finite real \(\lambda\). Thus two such generators with the same P must agree. This is precisely where self-adjointness excludes hidden rotations and alternative logarithms. The low-frequency spectral route is classical [R1]; (24) spells out its operator and domain assumptions rather than assuming every sampled process has such a generator.

**Step 3: recover local coefficients.** For a compact interval J choose \(f\in C_c^\infty\) equal to x on a neighborhood of J. The identified operator obeys

\[
 L_\theta(f^2)-2fL_\theta f=a_\theta(f')^2. \tag{25}
\]

This follows by differentiating \(f^2\) in the displayed differential generator; the drift terms cancel. On J, \(f'=1,f''=0\), so the left side identifies \(a_\theta\), and \(L_\theta f\) identifies \(b_\theta\). A countable cover of the line identifies both almost everywhere; their continuity and positive density identify their continuous versions. There is no need to apply L to an unbounded global coordinate outside its domain.

**Step 4: transport.** The observed type law gives \(\pi_\theta\). Bayes' rule gives occupancy weights \(\pi_\theta p_\theta(x)/p(x)\), proving (23), including its quantile endpoints. ∎

**How substantive is the class?** It includes stationary scalar OU processes and regular reversible scalar diffusions with a suitable self-adjoint realization. Formally, zero current gives
\(L f=(2p)^{-1}(ap f')'\), hence
\(\langle f,Lg\rangle_p=-\tfrac12\int ap f'g'\)
for compact tests. This verifies symmetry on the tests; **symmetry alone is not proof of self-adjointness of the generator or uniqueness of a boundary realization**. Those properties are assumed in Claim 7. One can specify them through a closed Dirichlet-form realization and its conservative diffusion, but no unproved general existence theorem for such a realization is being applied here. Claim 8 supplies a completely explicit nonempty subclass. Reflecting compact-interval models in [R1] provide related prior art, but their boundary local times and their convergence-rate assumptions are not silently substituted for (1).

An unobserved mixture of home levels or reversion rates generally fails the within-type scalar Markov premise. A known stationary mixture density does not repair that. Likewise interacting finite populations need not have scalar Markov coordinates. Observing more persistent covariates can make this assumption more plausible, but does not prove it. A Markov-looking sampled chain can still be Claim 3.

This is a population-law theorem with just one lag; it is not a claim that two noisy values for one endpoint numerically determine an arbitrary function. Nor is it a claim of stable recovery: division by a small characteristic function and logarithms near zero spectrum amplify error. For example, scalar spectral values \(r_n=e^{-n}\) and \(\widetilde r_n=e^{-2n}\) become arbitrarily close while their logarithms separate by n. This illustrates the inversion issue, not a diffusion-specific minimax lower bound. No noisy short-panel convergence rate is imported from [R1].

### 5.3 An explicit finite-lag subclass with unknown noise variance

**Claim 8 (typed stationary OU).** Conditional on an observed type \(\theta\), suppose all its endpoints obey

\[
 dX_i=-\kappa_\theta(X_i-\mu_\theta)dt+\sqrt{a_\theta}\,dW_i,
 \quad \kappa_\theta>0,\ a_\theta>0,
\]

at stationarity. Measurement errors are conditionally mean zero, independent of the entire latent path, have common type variance \(\tau_\theta^2\), and are uncorrelated between distinct dates. Their distribution need not be Gaussian. For three observed dates \(0,\Delta,2\Delta\), the exact conditional observation law identifies the parameters and \(a_B\).

**Proof.** Drop type subscripts. The explicit solution gives
\(\mu=\mathbb E Y_n\), \(v=a/(2\kappa)\), \(r=e^{-\kappa\Delta}\), and

\[
 \gamma_0=\operatorname{Var}(Y_n)=v+\tau^2,\quad
 \gamma_1=\operatorname{Cov}(Y_0,Y_1)=vr,\quad
 \gamma_2=\operatorname{Cov}(Y_0,Y_2)=vr^2.
\]

As \(v>0,0<r<1\),

\[
 r=\gamma_2/\gamma_1,\quad
 \kappa=-\Delta^{-1}\log r,\quad
 v=\gamma_1^2/\gamma_2,\quad
 a=2\kappa v,\quad \tau^2=\gamma_0-v. \tag{26}
\]

The parameters determine \(p_\theta=N(\mu_\theta,v_\theta)\). Type proportions and Bayes' rule in (23) determine occupancy and \(a_B\). These deductions prove uniqueness from the full law because equality of that law implies equality of these moments. ∎

This does not identify an endpoint-specific parameter distribution inside a type. It requires true stationarity conditional on the type, and uses the OU transition restriction. Claim 3 has exactly these sampled covariance formulas but lies outside that restriction. Serially correlated error adds unknown terms to \(\gamma_1,\gamma_2\), invalidating (26). Error means confounded with \(\mu_\theta\) also affect occupancy unless supplied independently.

Stability is conditional: (26) is continuous and locally Lipschitz on any compact parameter region with v and r bounded away from zero and \(r\le1-\epsilon\). If \(\gamma_1,\gamma_2\) are tiny, the divisions are poorly conditioned. With independent endpoint replicas in each type, increasing N at fixed three-date span consistently estimates these covariances by the law of large numbers; continuous mapping then gives pointwise consistency for (26) and the Gaussian-mixture band target. Finite second moments suffice for that LLN. Finite fourth moments support covariance precision calculations, but neither a uniform confidence guarantee nor a claim about the project's short panel is made here. Shared shocks require a different LLN. Small reversion relative to the observation span can still produce poor *relative* precision even when an absolute-error map is smooth.

## 6. An observation design that avoids the Markov restriction

**Claim 9 (identified short-lag occupancy limit).** Fix a stationary marginal p and an interior band B with positive mass. Suppose (1) holds and for the event \(C_B=\{X_I(t)\in J_B\}\),

\[
 \sup_{0\le s\le h_0}\mathbb E[\beta_I(t+s)^2\mid C_B]<\infty,\qquad
 \sup_{0\le s\le h_0}\mathbb E[a_I(t+s)\mid C_B]<\infty,
\]

\[
 \frac1h\int_0^h\mathbb E[|a_I(t+s)-a_I(t)|\mid C_B]ds\longrightarrow0.
 \tag{27}
\]

Suppose an added experiment identifies \(\mathcal L(X_I(t),X_I(t+h_j))\) for a sequence \(h_j\downarrow0\), using error-free observations or Claim 6 at each lag. Then it identifies \(a_B\) without assuming X is Markov:

\[
 a_B=\lim_{j\to\infty}\frac1{h_j}
 \mathbb E[(X_I(t+h_j)-X_I(t))^2\mid X_I(t)\in J_B]. \tag{28}
\]

**Proof.** The recovered marginal identifies p and the quantile interval \(J_B\); thus each conditional second moment in (28) is a functional of a recovered joint law. The conditional isometry gives
\(\mathbb E[Z_h^2\mid C_B]=\mathbb E\int_t^{t+h}a_I(s)ds\).
The conditional squared drift divided by h is bounded by \(K_\beta^2h\); the absolute cross term divided by h is at most \(2K_\beta\sqrt{\bar a h}\), exactly as in Claim 5. Condition (27) makes the integrated bracket divided by h converge to \(a_B\). This proves (28). Using a conditional variance instead changes the expression by \((\mathbb E[B_h\mid C_B])^2/h\le K_\beta^2h\), which also vanishes. ∎

Sufficient replacement for oracle occupancy is therefore **recovery of the latent joint law**, not classifying each endpoint perfectly. Independent replicate *vectors* can identify the start-state distribution and its joint movement with serially correlated errors, subject to Claim 6(b). Known-error deconvolution also works and does not require the latent characteristic function to be nonvanishing.

To identify the pointwise coefficient, assume the conditions of Claim 9 on each sufficiently small latent x-neighborhood, and shrink those neighborhoods after the short-lag limit. If \(a_*\) and p are continuous and p(x)>0, their conditional average over \([x-\epsilon,x+\epsilon]\) tends to \(a_*(x)\) by continuity and division by the interval mass. Without continuity the statement holds only at the appropriate Lebesgue points, rather than at every x. A joint statistical limit would need uniform control as both lag and occupancy bandwidth shrink; it is not supplied by this iterated population-law argument.

Claim 9 concerns a family of experiments with genuinely smaller physical lags, not more observations separated by the same week. At fixed finite lag, only bounds such as (14) follow absent structural assumptions. Daily data may improve a bound without making it zero; daily totals also differ from point measurements. Noise-robust high-frequency quadratic-variation theory [R6] is relevant prior art but cannot certify a weekly finite-difference statistic. A fixed finite-span path's realized occupation/bracket ratio is generally random; learning a stationary population expectation additionally needs independent replicas, ergodicity over growing span, or another valid averaging experiment.

## 7. Permanent labels and the occupancy transport

For any persistent type \(\theta\), define

\[
 g_\theta(x)=\mathbb E[a_I(t)\mid\theta_I=\theta,X_I(t)=x],\qquad
 w_\theta(x)=\frac{\pi_\theta p_\theta(x)}{p(x)}.
\]

Then \(a_*(x)=\sum_\theta w_\theta(x)g_\theta(x)\). In a band,

\[
 w_{\theta B}=P(\theta_I=\theta\mid X_I(t)\in J_B),\qquad
 a_B=\sum_\theta w_{\theta B}
       \mathbb E[a_I(t)\mid\theta_I=\theta,X_I(t)\in J_B]. \tag{29}
\]

Only if within-type variance is constant, or the requisite conditional-mean independence is justified, may the inner expectation be replaced by the permanent-type mean \(\alpha_\theta=\mathbb E[a_I(t)\mid\theta]\).

Even constant-type variances do not license using cohort number as current rank. For two equally prevalent OU types with distinct means \(\mu_1<\mu_2\), common stationary variance v, and different instantaneous variances, the occupancy weight
\(p_2(x)/(p_1(x)+p_2(x))\) varies with x and lies strictly between zero and one at every finite x. Each permanent type has its own constant variance, but no finite current-x band consists exactly of that type.

There is a second, logically distinct, moment failure even with perfect occupancy information: cohort means do not determine within-cohort state dependence. Here is a fully explicit stationary example. One coarse permanent cohort contains equal proportions of two fixed subtypes H=−m,+m, with m,v>0. In model A, each subtype follows an OU with mean H, instantaneous variance 1, and reversion 1/(2v). In model B, its instantaneous variance is \(1+\epsilon H/m\), \(0<\epsilon<1\), and its reversion is that variance divided by 2v. Initialize all these OU processes at stationarity. Both models have the same subtype occupancy densities \(N(H,v)\), the same overall density \(p=\tfrac12p_-+\tfrac12p_+\), and the same coarse-cohort mean instantaneous variance 1. But

\[
 a_{*,A}(x)=1,\qquad
 a_{*,B}(x)=1+\epsilon\frac{p_+(x)-p_-(x)}{p_+(x)+p_-(x)}
          =1+\epsilon\tanh(mx/v).
\]

The last equality follows from \(p_+(x)/p_-(x)=e^{2mx/v}\). Every band wholly in x>0 therefore has a different variance. Thus even constant *named* variances can induce state dependence within an inadequately resolved cohort; truly state-varying named variances require the same conditional treatment. This example is a **moment/transport failure**, not equality of full observation laws. Retaining the subtype coefficients separately would repair this specific loss; keeping only their coarse average cannot.

**Claim 10 (transport error).** Suppose finite types have reference coefficients \(\alpha_\theta\in[\alpha_{\min},\alpha_{\max}]\), estimates with \(|\widehat\alpha_\theta-\alpha_\theta|\le\epsilon_\theta\), occupancy probability vectors w and \(\widehat w\), and

\[
 \left|\mathbb E[a_I\mid\theta,X_I\in J_B]-\alpha_\theta\right|\le d_{\theta B},
 \qquad \|\widehat w-w\|_1\le\rho_B.
\]

For \(\widehat a_B=\sum_\theta\widehat w_\theta\widehat\alpha_\theta\),

\[
 |\widehat a_B-a_B|\le
 \sum_\theta\widehat w_\theta\epsilon_\theta
 +\frac{\alpha_{\max}-\alpha_{\min}}2\rho_B
 +\sum_\theta w_\theta d_{\theta B}. \tag{30}
\]

**Proof.** Add and subtract \(\sum\widehat w_\theta\alpha_\theta\) and \(\sum w_\theta\alpha_\theta\). Bound the first and last differences directly. For the middle, the weights' differences sum to zero, so subtract the midrange \((\alpha_{\max}+\alpha_{\min})/2\) from every coefficient and apply the \(\ell^1\) bound. ∎

The \(\epsilon_\theta\) can contain the finite-lag bounds of Claim 5 plus a separately justified sampling-error bound. The occupancy error \(\rho_B\) must include error in latent quantile boundaries and type assignment, not just multinomial uncertainty conditional on known boundaries. The final term survives perfect estimation and infinite samples unless the within-type restriction is true. A similarly defined pointwise bound holds with state-conditional weights and errors, but band averages do not imply it.

Estimating named coefficients on permanent cohorts and then transporting their joint law to latent occupancy is compatible with the project rule against estimating movement by naive observed current rank. It is an additional calculation, not a reason to regress increments on their own noisy starting rank. Latent-class identification from short panels has substantial prior art [R7]; adjacent observations from an OU are not conditionally independent given only a permanent label, so the repeated-measurement mixture theorems cannot simply be applied to them.

## 8. Sampling, overlap, common shocks, selection, and missingness

### 8.1 Overlap is uncertainty and can also affect centering bias

For constant-variance Brownian motion let \(Z_k=X(t_k+h)-X(t_k)-\mu h\), \(t_k=k\Delta\), k=0,…,m−1. Brownian independent increments give

\[
 \operatorname{Cov}(Z_k,Z_l)=a(h-|k-l|\Delta)_+,\qquad
 \operatorname{Var}(\overline Z)=\frac a{m^2}
 \left[mh+2\sum_{d=1}^{m-1}(m-d)(h-d\Delta)_+\right]. \tag{31}
\]

The covariance equals a times interval-overlap length; summing all pairs proves the second expression. Consequently

\[
 \mathbb E\left[\frac1m\sum_k(Z_k-\overline Z)^2\right]
 =ah-\operatorname{Var}(\overline Z). \tag{32}
\]

Thus a finite-window endpoint mean can be large without a permanent endpoint trend. Removing its observed contribution subtracts real diffusion. The usual factor m/(m−1) corrects independent-observation centering, not the overlap terms.

For centered Gaussian increments with covariance matrix K,

\[
 \operatorname{Var}\left(m^{-1}\sum_k Z_k^2\right)
       =2m^{-2}\sum_{k,l}K_{kl}^2. \tag{33}
\]

To verify the Gaussian fourth-moment identity, write one member of a Gaussian pair as a multiple of the other plus an independent normal; expanding gives
\(\mathbb E[Z_k^2Z_l^2]=K_{kk}K_{ll}+2K_{kl}^2\).
This proves (33) and exposes effective sample size without stochastic simulation.

Across both names and times the exact general formula is
\(\operatorname{Var}(\sum_rw_rU_r)=\sum_{r,s}w_rw_s\operatorname{Cov}(U_r,U_s)\)
for whatever increment score U is being averaged. With shared shocks, the cross-name terms can remain nonzero as N grows. Blocks of overlapping starts do not create independent weeks. A cross-section with a common volatility environment can converge to its conditional, realized-environment moment rather than the unconditional stationary target.

### 8.2 Three distinct limits

| Experiment | What can improve under further sampling assumptions | What it does not repair |
|---|---|---|
| N→∞, fixed \(\Delta,T\) | Finite-dimensional panel-law and moment estimation with genuinely independent or sufficiently weakly dependent replicas; Claim 8 is an explicit example | Either counterexample, fixed-lag bias, cohort/occupancy mismatch, a realized common environment |
| T→∞, fixed \(\Delta\) | The sampled process law under appropriate ergodicity; sufficient input for low-frequency spectral methods in an identified class | Subweekly aliases in Claim 3; arbitrary signal/noise allocation; a false within-type Markov restriction |
| \(\Delta\to0\) | Access to instantaneous bracket information after proper noise handling, as in Claim 9 | Unrestricted persistent observation noise; reliable unconditional occupancy or drift from one short realized path; population turnover ignored by the model |

No universal mixing or independence property was assumed in (1), so no universal consistency theorem for the empirical A follows from it. Claims 5 and 10 separate deterministic model/instrument error from whatever sampling confidence statement a later design can justify.

### 8.3 Changing eligible pairs and other observational extensions

If lag h uses pair law \(P_h\) instead of one common P, it changes drift, volatility, error, and label composition. It is then not (3). For a bounded increment variable \(|Z|\le L\) and total variation \(d_{\rm TV}(P_h,P)\le\rho\), the elementary bound
\(|\operatorname{Var}_{P_h}Z-\operatorname{Var}_PZ|\le6L^2\rho\)
follows by bounding the second-moment difference by \(2L^2\rho\) and the squared-mean difference by \(4L^2\rho\). Unbounded diffusion increments require a tail/uniform-integrability condition instead; no bounded-increment assumption is being made for the Gaussian examples. A common eligible mask prevents this particular composition change but does not remove (11).

Missingness depending on the latent level or future survival changes the observed pair law. Full-law recovery could be extended with a specified identifiable selection model, or under suitable missing-at-random assumptions with known/identified positive observation probabilities and inverse weighting. No such assumptions are supplied here, so the results do not identify the uncensored population from a top-K or survivor-only panel. Estimated permanent labels require the joint misclassification/calibration law; separate windows by themselves do not provide it.

Finally, weekly aggregation, the log(1+count) transformation, and finite-N ordered-gap summaries need their own measurement maps. The smooth marginal quantile in (8) is not automatically a time-median empirical ladder or an expected ordered gap. No finite-particle Atlas product-gap theorem is used in this note.

## 9. Consequences for structural reconstruction

**Claim 11 (conditional propagation; pointwise and band targets differ).** On a compact quantile interval \(B=[u_1,u_2]\), assume (8), \(G(u)\ge g_0>0\), and put \(a(u)=a_*(x(u))\). With a shared anchor \(x(u_0)\),

\[
 x(u)=x(u_0)-\int_{u_0}^u\frac{a(v)}{2G(v)}dv. \tag{34}
\]

If \(\|\widehat a-a\|_\infty\le\epsilon_a\), G is independently known exactly, and reconstructed slopes use (8), then

\[
 |\widehat S(u)-S(u)|\le\frac{u\epsilon_a}{2g_0},\qquad
 |\widehat x(u)-x(u)|\le\frac{|u-u_0|\epsilon_a}{2g_0}. \tag{35}
\]

If also \(a\le A_0\), \(\|\widehat G-G\|_\infty\le\epsilon_G<g_0\), then

\[
 \left|\frac{\widehat a}{2\widehat G}-\frac a{2G}\right|
 \le\frac{\epsilon_a}{2(g_0-\epsilon_G)}
       +\frac{A_0\epsilon_G}{2g_0(g_0-\epsilon_G)}. \tag{36}
\]

Multiplying by u gives the slope bound; multiplying by \(|u-u_0|\) bounds the anchored log-ladder error. More generally integrate the corresponding nonuniform errors. An uncertain anchor adds its absolute error.

**Proof.** Equation (8) implies \(dx/du=-a/(2G)\). Integrate and subtract. For (36), split the difference into \((\widehat a-a)/(2\widehat G)+a(G-\widehat G)/(2G\widehat G)\), then use the positive lower bounds. ∎

**Only a band average known.** Claim 9, or (30), alone identifies/controls \(a_B\); it does not control \(\|\widehat a-a\|_\infty\). The exact structural restriction is instead

\[
 \int_{u_1}^{u_2}\frac{2G(u)S(u)}u\,du=m_Ba_B. \tag{37}
\]

If \(G\in[g_{\min},g_{\max}]\) and \(a\ge0\),

\[
 \frac{m_Ba_B}{2g_{\max}}
 \le x(u_1)-x(u_2)
 \le\frac{m_Ba_B}{2g_{\min}}. \tag{38}
\]

This follows by bounding 1/G in (34). An identified enclosure \([a_-,a_+]\) gives the corresponding lower/upper bounds using \(\max(0,a_-)\) and \(a_+\). For constant G on the band, its log-ladder drop is exactly \(m_Ba_B/(2G)\). For varying G, a band mean alone does not specify the weighted integral; a narrow concentration of a at different u can change it. No pointwise slope reconstruction is claimed from a band mean.

The identification theorems use movement transitions, and none sets a coefficient equal to \(2GS/u\) to match an observed ladder. Nevertheless recovering invariant densities and checking their balance on the **same** stationary sample is not an independent mechanistic test. Independent movement/noise information, separately justified signed drift/current and centering, and a held-out structural implication are needed to turn the relationship into empirical evidence. In particular, G inferred by rearranging the observed variance–slope identity is not independently measured G.

## 10. Assumptions mapped to observations and restrictions

| Assumption or input | Where it is needed | Sufficient information/design; remaining restriction |
|---|---|---|
| Relative coordinate and covariance | (1), (4), every structural target | Declared known center or an independently characterized centering instrument; otherwise include its error and cross terms |
| Common nonanticipating pair distribution | (3), Claims 4–5 | Fixed eligible starts/names and weights shared by lags; future-survival selection needs a selection model |
| Calibration available before evaluation | Conditional martingale isometry | A joint calibration/observation law and a valid filtration; a time gap alone is insufficient |
| Signal-independent known error-vector law, nowhere-zero characteristic function | Claim 6(a), Claim 7 | Independent noise instrument that identifies **joint** errors at relevant dates; marginal noise variance is insufficient |
| Mutually independent replicate vectors and latent vector; mean normalization; nonvanishing conditions | Claim 6(b) | Two channels observing the same latent values with independent full error paths; same-panel common error violates this |
| Observed persistent types, correct within-type law | Claims 7–8, transport | External type information or a separately proved latent-type identification theorem; permanent ranks need not be sufficient types |
| Scalar stationary reversible Markov diffusion, generator domain | Claim 7 | A modeling restriction on the continuous-time coordinate, not verifiable from a single lattice alone; Claim 3 defeats automatic inference |
| Exact OU within each type, uncorrelated errors over time | Claim 8 | Three-date conditional covariance law; neither heterogeneous homes inside type nor serial error is allowed unnoticed |
| Drift/bracket/error bounds | Claim 5 | Independent scientific restrictions or identified bounds; otherwise (15) can be vacuous |
| Lags tending to zero and conditional continuity | Claim 9 | New short-lag measurements with noise-managed joint laws; no scalar Markov restriction needed |
| Occupancy weights and within-type conditional variance | (23), (29)–(30) | Latent type/state joint law or a valid deconvolution/occupancy model; cohort mean variance alone cannot supply it |
| Zero current, boundary limits, no jumps/turnover | Claim 1 and reconstruction | A justified population and coordinate model plus separate current/turnover evidence; stable appearance is insufficient |
| Independent G bounded away from zero | Claims 11, (35)–(38) | Independent signed drift and boundary/current information; variance–slope calibration is circular |
| Sampling LLN and precision | Estimation, not identification | Independent endpoint replicas, suitable dependence bounds, or long-span ergodicity; N×T is not automatically an independent sample size |

## 11. Prior art and source verification

All links below were accessed on 2026-10-12. “READ THIS SESSION” specifies the portion actually inspected, not the whole paper. The proofs above are given explicitly. Standard foundations used are Itô's formula/isometry, conditional expectation, characteristic-function uniqueness, and self-adjoint spectral calculus (whose exact operator form and domain are stated in (24)). No uninspected specialist theorem supplies a missing proof step. No original claim of priority is made for the constructions or their combination.

**[R1] READ THIS SESSION — Gobet, Hoffmann and Reiß (2004), _Nonparametric estimation of scalar diffusions based on low frequency data_, Annals of Statistics 32, 2223–2253, [original article reprint](https://arxiv.org/pdf/math/0503680), §§1.1–1.2, Assumption 2.1, Definition 2.3, Theorem 2.4.** The low-frequency semigroup/spectral identification route predates this note. Their statistical results concern a stationary scalar diffusion reflected on [0,1], positive diffusion, bounded drift, and specified Sobolev classes, with fixed sampling interval and growing time series. Claim 7 uses the same operator principle under its own explicit domain/reversibility assumptions, and proves coefficient extraction. Their reflected-boundary model is not literally (1) on the line; their noiseless asymptotic rates are not claimed for noisy heterogeneous short panels.

**[R2] READ THIS SESSION — Kotlarski (1967), _On characterizing the gamma and the normal distribution_, Pacific Journal of Mathematics 20, 69–76, [original paper](https://msp.org/pjm/1967/20-1/pjm-v20-n1-p08-p.pdf), Lemma 1 and proof, printed pp.70–72, and Remarks 1–2.** The lemma treats three independent variables observed through two differences, with nonvanishing joint characteristic function, and identifies their distributions up to location. Claim 6(b) uses the equivalent additive measurement structure, a mean normalization, first moments, and an explicitly proved vector derivative formula. It is a sufficient version, not a claim to the weakest known assumptions.

**[R3] READ THIS SESSION — Brunick and Shreve (2013), _Mimicking an Itô process by a solution of a stochastic differential equation_, [author-hosted article](https://www.math.cmu.edu/users/shreve/MimickingItoProcess.pdf), Theorem 3.6, Corollary 3.7, Example 3.8, and the uniqueness/Markov discussion preceding Corollary 3.13.** Corollary 3.7 assumes the adapted Itô representation and finite expected integrals of absolute drift and covariance from Theorem 3.6; its conditional coefficients agree outside a Lebesgue-null time set and yield matching one-time marginals. It does not say the original process is Markov or preserve its transition law. Claim 1 proves only the weaker stationary test-function identity it needs. Additional uniqueness and regularity assumptions for a Markov mimicker are not being imposed on the original process by implication.

**[R4] READ THIS SESSION — Jourdain and Reygner, _Capital distribution and portfolio performance in the mean-field Atlas model_, [arXiv original](https://arxiv.org/pdf/1312.5660), equilibrium conditions (E1)–(E2), stationary quantile integral and Theorem 2.4.** In this PDF the long-time subsection is §2.3, not §2.4 as the map suggests. Theorem 2.4 requires uniform ellipticity, positive integrated centered rank drift, an endpoint integrability condition, C²/C¹ variance/drift with Hölder highest derivatives, and moment/Wasserstein conditions on initialization. Its mean-field convergence result is not used to assert stationarity or finite-N accuracy here. The known quantile relation is used only under the separate boundary/current assumptions proved in Claim 1.

**[R5] READ THIS SESSION — Evdokimov and White (2012), _Some extensions of a lemma of Kotlarski_, [author manuscript dated July 25, 2011](https://www.mit.edu/~kevdokim/kotlarski-note-final.pdf), introduction, Assumptions A/B and Lemmas 1/2.** Their extensions permit characteristic-function zeros under specific conditions: the first error's zeros are not shared by its derivative, the second has isolated zeros, or an alternative exponential-tail restriction is supplied. These specialist extensions are prior art only. Claim 6 states stronger directly proved assumptions and does not claim that nonvanishing is necessary.

**[R6] READ THIS SESSION — Zhang, Mykland and Aït-Sahalia (2005), _A tale of two time scales: determining integrated volatility with noisy high-frequency data_, [author-hosted paper](https://swh.princeton.edu/~yacine/twoscales.pdf), observation model (4), introduction's asymptotic formulas, and §§3.5/4.1 discussion.** Independent observation noise and increasing sampling frequency over a fixed interval underlie its integrated-volatility setting. No consistency theorem from it is applied to fixed weekly lags. Claim 9 is instead a directly proved short-lag population-moment result, with neither that estimator nor its limiting distribution invoked.

**[R7] READ THIS SESSION — Bonhomme, Jochmans and Robin (2016), _Estimating multivariate latent-structure models_, [author-hosted manuscript](https://jochmans.github.io/publications/latent%20structures/jade.pdf), §§2.1–2.3 and Theorems 1–3.** Their latent-class recovery uses a known finite number of classes, appropriate multilinear/conditional-independence structure, full-column-rank matrices, observable lower-order arrays, and column separation, with a common permutation ambiguity. No such theorem identifies arbitrary endpoint heterogeneity in this note. Conditional OU serial dependence is not conditional independence given permanent class. Claim 7 deliberately requires observed sufficient types.

**[R8] READ THIS SESSION — Gassiat, Le Corff and Lehéricy (2020), _Identifiability and consistent estimation of nonparametric translation hidden Markov models with general state space_, [JMLR original](https://www.jmlr.org/papers/volume21/19-802/19-802.pdf), model and §2 (H1, Theorems 1–2, Corollary 4).** This supplies important context: known noise or replicates are not necessary in every restricted class. Their observation noise is iid and independent of the latent sequence. Theorem 1 imposes unique stationary laws, Laplace growth exponent below 2, and H1 (no simultaneous identically-zero transform slices); the scalar Theorem 2 permits exponent below 3 with transition densities and excludes iid Gaussian hidden chains. These are noise/skeleton identification results, not removal of continuous-time aliases. They are not invoked to weaken Claim 7's noise assumptions or certify the project's observation mechanism.

**[R9] READ THIS SESSION — Gassiat, Le Corff and Lehéricy, [author-hosted erratum to their 2022 multivariate deconvolution paper](https://www.imo.universite-paris-saclay.fr/~elisabeth.gassiat/erratum.pdf), opening explanation and stated correction scope, pp.1–4.** The erratum identifies an error in a stability-inequality proof and supplies restricted replacements/additional assumptions; it explicitly distinguishes consistency from rates. No result from the 2022 paper or this correction is a proof dependency here. This source check is another reason not to infer a usable rate from an identification citation.

**[R10] READ THIS SESSION — ABSTRACT ONLY; NO THEOREM VERIFIED — Yeo (2026), _Identifiability, Fisher Information, and Amortized Inference for Heterogeneous Diffusion from Discrete-Time Noisy Observations_, [publisher page](https://proceedings.mlr.press/v327/yeo26a.html), publication metadata and abstract.** The abstract describes temporal-correlation identification and heterogeneous diffusion/noise models. Both attempted PDF URLs were unavailable through the browser. It is a current primary-literature lead, not an externally verified proof dependency, and no theorem, precision bound, or novelty comparison is inferred from it.

The search covered low-frequency scalar diffusion identification, translation HMMs with measurement error, repeated measurements, short-panel heterogeneity, and conditional projection, including current author publication lists and the 2026 lead. It was not an exhaustive novelty review. Finite Atlas product-gap laws, collision estimators, and turnover results from the source map are not load-bearing for this proof and are not asserted from the map alone. No citation tagged SEARCH SUMMARY ONLY or KNOWN FROM TRAINING AND NOT RE-READ is used as a specialist proof dependency.

## 12. Measurement conclusion and exact remaining obligations

The present two-lag permanent-cohort instrument can be made interpretable as a cohort bracket estimate only with independently justified control of noise covariance, drift heterogeneity/reversion, drift–martingale covariance, and changing composition. Even that success would leave an occupancy transport problem. A correlation with ladder slope does not supply any of these missing restrictions.

Two concrete sufficient routes are proved:

1. **Retain weekly sampling and restrict the latent model.** Supply an identifying error instrument and observed sufficient endpoint types; assume the scalar within-type coordinate is a stationary reversible Markov diffusion with the stated generator domain. Recover transitions and the generator, then transport state-specific variance with the latent occupancy law. Claim 8 is a simpler three-date OU route with weaker knowledge of the error law but stronger dynamics/noise restrictions. Infinite-data identification is exact; finite-sample inversion and continuous-time model validity remain separate.
2. **Add a short-lag observation experiment.** Obtain same-state independent measurement channels or a known error law at successively smaller physical lags, recover latent start/movement joint laws, and use Claim 9. This removes the scalar Markov requirement under conditional moment continuity. It still requires enough independent population or time information for stationary averaging, and reliable centering, population coverage, and drift/current information.

For either route, independently supplied G bounded away from zero converts *pointwise* variance error into (35)–(36). If only a band average is obtained, the defensible structural restriction is (37) or (38). Neither route by itself identifies a causal explanation of why endpoint drift or variance takes its measured form.

The following remain unresolved for application; the first missing mathematical or observational step is stated explicitly:

| Unresolved assertion | First missing step |
|---|---|
| The project's existing noisy weekly law identifies \(a_B\) | Specify an observation/dynamic class and establish an assumption that excludes **both** Claims 2 and 3; neither the two-lag relation nor a stationary ladder does so |
| Permanent cohorts are sufficient types for Claim 7 or 8 | Establish the within-type scalar continuous-time Markov law, or identify additional persistent/hidden states and prove generator uniqueness for that enlarged class |
| Cohort coefficients transport to current occupancy | Identify the joint law of latent state, type, and instantaneous variance, or justify and bound the last term in (30) |
| A practical nonparametric weekly estimator has calibrated precision | Prove stable regularized inversion and a sampling theorem for the actual dependent noisy finite panel, including calibration and quantile estimation; injectivity alone is not that lemma |
| Replicate measurements supply the required noise instrument | Establish independence of the two full error vectors from each other and from latent values under cohort selection, or supply a different proved identification model |
| A usable finite-week enclosure is narrow | Supply defensible numerical bounds for \(K_\beta,\bar a,\eta,\delta_N,\delta_H\) without learning them from the target ladder |
| Short-lag population identification yields finite-span inference | Prove a joint lag/noise/occupancy-bandwidth sampling limit, including cross-name dependence; Claim 9 proves an iterated population-law limit only |
| Stationary G is independently known and no current is omitted | Identify signed drift in the same relative coordinate and justify upper-boundary/current conditions; entry, exit and jumps require a changed balance model |
| The continuum marginal target equals the project's finite ordered ladder | Prove or quantify the finite-N, time-aggregation, censoring and ladder-functional mapping |
| These constructions or combinations are publishably novel | Complete a focused priority search and obtain independent mathematical scrutiny; neither was authorized as additional agent work here |

**What was not done.** No empirical data or sealed folders were opened; no model code, fits, stochastic simulations, gates, model health scripts, external jobs, or additional agents were run. No defaults, protocols, ledgers, program state, paper claims, or adoption decisions were changed. The result is a theoretical handoff, with a separate record entry for the designated record writer. The two nonidentification results and the restricted positive proofs are complete as stated; applicability, statistical rates for the project, and causal interpretation remain unestablished.
