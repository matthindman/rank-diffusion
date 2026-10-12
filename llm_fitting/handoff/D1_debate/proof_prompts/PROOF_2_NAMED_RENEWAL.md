# Prompt 2 — stable rank structure from named heterogeneity and renewal

Investigate whether the following explicit interacting population has a unique attracting stationary distribution and a controlled stationary rank-size limit. Seek a theorem or counterexample, not confirmation of the empirical rank-diffusion hypothesis. The scientific purpose is to understand when independently specified movement and replacement generate structure without a prescribed target ladder or a personal restoring home.

## Starting model

There are N slots, with fixed types theta in {1,...,m}; N_theta/N tends to q_theta>0 and sum q_theta=1. A slot contains an endpoint until replacement, which creates a new endpoint identity of the same type. An endpoint keeps its type throughout its life. Type-preserving replacement is a modeling restriction, not an empirical finding.

On the real line, between replacements,

    dX_i = b_theta_i(u_N(X_i)) dt + sigma_theta_i dW_i,
    u_N(x) = N^(-1) sum_j 1{X_j > x}.

Use independent Brownian drivers, positive type-specific variances a_theta=sigma_theta^2 bounded above and away from zero, and bounded Lipschitz rank-drift profiles b_theta on [0,1]. Define the finite-N rank convention at ties and verify well-posedness rather than overlooking its discontinuities. Each slot independently resets at constant rate lambda_theta>0 to a draw from a fixed smooth probability density nu_theta with finite second moment, independently of its previous state and the Brownian motions. All inputs are specified without using the target stationary ladder. The reset distribution supplies a location scale; it is not inferred from the future ladder. These X coordinates use that declared fixed origin, not a continuously subtracted empirical mean.

The proposed mean-field type densities p_theta integrate to q_theta, and u(x)=sum_theta integral_x^infinity p_theta(y)dy. Their candidate stationary equations are

    0 = -(b_theta(u(x)) p_theta(x))'
        + (a_theta/2) p_theta''(x)
        - lambda_theta p_theta(x) + lambda_theta q_theta nu_theta(x).

The time-dependent equation replaces zero by partial_t p_theta. First verify that this is the correct limit equation and source normalization under the specified replacement rule.

## Target and scope

Aim for a sufficient, checkable condition on these inputs under which the nonlinear evolution has one stationary law and converges to it at a quantified rate. A proved condition restricting interaction strength relative to renewal is useful even if not necessary. Do not presume positive renewal alone guarantees uniqueness of this nonlinear mean-field limit; finite-N regeneration does not by itself prove uniform-in-N convergence or uniqueness after taking N to infinity.

Begin with the noninteracting case as a check and identify existing results. Then address a genuinely interacting, heterogeneous class. If the proposed general class is too broad or the claim false, provide a concrete obstruction or counterexample and prove a relevant restricted theorem. Do not turn every drift profile into an imposed stationary quantile or reset to the desired full population law. A bounded reflecting domain, identical types, or a contraction assumption is an explicit restriction whose scientific cost must be stated.

After establishing the central stability result, attempt a finite-N stationary approximation on a fixed interior quantile interval [epsilon,1-epsilon]. State the probability metric, mode of convergence, constants, and assumptions translating distribution error into quantile error, including any lower density bound. A finite-time propagation result is not sufficient to exchange the limits t→infinity and N→infinity. Identify that missing step if it remains unresolved. Do not claim uniform control of the extreme head from an interior theorem.

Derive the aggregate stationary current and the occupancy-weighted coefficients from the type densities. With p=sum p_theta, J=sum[b_theta(u)p_theta-(a_theta/2)p_theta'], define G(u)=-integral_0^u b_eff(x(v))dv. Under the necessary upper-boundary conditions, check

    a_eff(x) p(x) = 2G(u(x)) + 2 integral_x^infinity J(y)dy.

State all boundary terms if these conditions fail. Recover the ladder by quantiles of p; distinguish this from expected finite-N gaps or a time-median ladder. Also report the implied type occupancy P(theta|X=x)=p_theta(x)/p(x), so the result concerns named characteristics as well as shape. Structural agreement alone does not establish correct identity persistence or social causation.

If a mixing bound is obtained, retain its dependence on renewal, diffusion, drift, and tails. Do not identify a fitted drift ratio's reciprocal with relaxation time. Consider reconstruction stability under perturbing independently supplied coefficients only after the main theorem; report ill conditioning rather than assuming it away. Common noise, state-dependent replacement, redraw of type, and jumps are later extensions, not requirements for this first proof.

## Proof and research conditions

Use current primary literature on rank-based diffusions, hybrid Atlas models, nonlinear Fokker–Planck equations, resetting/renewal, and uniform propagation of chaos. Check whether a known theorem already supplies the result before claiming novelty. The project's THEORY.md is a lead list, not a proved premise. For each dependency identify the precise theorem and check its hypotheses; tag citations as READ THIS SESSION (portion), KNOWN FROM TRAINING AND NOT RE-READ, or SEARCH SUMMARY ONLY. Unretrieved references are leads, not verified dependencies.

Pursue a complete argument using your chosen methods. Return an exact theorem or counterexample, explicit assumptions, a lemma dependency list, the proof, the relation to prior work, the unresolved obligations, and what would have to be independently measured to apply the result. A useful restricted theorem is preferable to a purported general proof with unproved compactness, uniformity, or uniqueness steps. Keep formal proof status separate from natural-language review status.

Do not run empirical analysis, model fits, stochastic simulations, or gate evaluations. Do not edit model code or start agents. Honor project working-copy/write restrictions. At a resource limit, leave a mathematically precise checkpoint; do not report a conjectured step as proved.
