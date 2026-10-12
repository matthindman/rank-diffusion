"""V4 frozen kernel with two read-only output arrays.

Copied from planted_sim.simulate at bbc8645. Only the function name and
allocation/writing of full_latent and full_amp differ. Original V2 source
is unchanged; tests compare every original output under common seeds.
"""
from __future__ import annotations
import numpy as np
from .planted_sim import MECHANISMS, value, extend_homes, clip_variance

def simulate_with_layers(p,T,seed=0,*,mechanism="baseline",strength=0,track=4000,
             top_record=None,burn=40,return_full=False):
    """Forward Euler OU, AR layers, observation noise and replacement identities.

    Coefficients use the rank of the latent lasting level at the beginning of
    each update. Homes remain endpoint-specific; replacements receive new homes
    and new identities. Independent random streams separate the physical layers.
    """
    if mechanism not in MECHANISMS: raise ValueError(mechanism)
    if T<1 or burn<0: raise ValueError("Invalid duration")
    if MECHANISMS[mechanism] is not None and strength not in (0,1):
        raise ValueError("Strength must be 0 or 1")
    amount=None if MECHANISMS[mechanism] is None else MECHANISMS[mechanism][strength]
    n=int(value(p,"N"));zk=np.asarray(value(p,"z_knots"),float)
    streams=[np.random.default_rng(s) for s in np.random.SeedSequence(seed).spawn(12)]
    amp, perm, fast, med, obs, fac, deaths, rebirth, extra, vol, off_rng, select=streams
    home=extend_homes(value(p,"w0"),n);level=home.copy()
    fast_state=np.zeros(n);medium_state=np.zeros(n);slow_state=np.zeros(n)
    identities=np.arange(n,dtype=np.int64);next_identity=n
    s=float(value(p,"temper_s",0));b=float(value(p,"mix_b",0))
    def amplitudes(size):
        return np.exp(-s*s/4+s*amp.standard_normal(size)/2) if s>0 else np.ones(size)
    sqrt_v=amplitudes(n)
    def lasting_amplitude(v):
        return v**b*np.exp(-(b*b-b)*s*s/4) if s>0 and b>0 else np.ones_like(v)
    sqrt_w=lasting_amplitude(sqrt_v)
    common=0.;rho=value(p,"factor_rho")
    df=float(value(p,"t_df",np.inf))
    second=value(p,"sigma_trans2")
    has_second=second is not None and np.any(np.asarray(second)>0)
    bound_norm=np.sqrt(clip_variance(df,amount)) if mechanism=="bounded_fast" else 1.
    is_off=np.zeros(n,dtype=bool)
    tau=amount if mechanism=="log_volatility" else 0.
    log_v=-tau*tau/2+tau*vol.standard_normal(n)
    ntrack=min(track,n);selected=np.sort(select.choice(n,ntrack,replace=False))
    depth=min(n,max(100,top_record or 0))
    out={"tvals":np.empty((T,ntrack)),"tranks":np.empty((T,ntrack),np.int32),
         "tids":np.empty((T,ntrack),np.int64),"top_ids":np.empty((T,depth),np.int64),
         "ranksize":np.empty((T,min(2000,n)))}
    if return_full:
        out.update(full_values=np.empty((T,n)),full_ranks=np.empty((T,n),np.int32),
                   full_ids=np.empty((T,n),np.int64),
                   full_latent=np.empty((T,n)),full_amp=np.empty((T,n)))
    def interp(name,z,default=0.):
        a=value(p,name)
        return np.full(n,default) if a is None else np.interp(z,zk,np.asarray(a,float))
    def fast_draw():
        z=fast.standard_normal(n)
        if mechanism in ("gaussian_control","log_volatility") or not np.isfinite(df):
            return z
        return z*np.sqrt((df-2)/fast.chisquare(df,n))
    for tt in range(-burn,T):
        ordering=np.argsort(-level);prank=np.empty(n,np.int64)
        prank[ordering]=np.arange(1,n+1)
        z=np.log(np.clip((prank-.5)/n,1e-6,1.))
        k=interp("kappa_z",z,float(value(p,"kappa",0)))
        sp=interp("sigma_perm",z);phi=interp("phi",z)
        st=interp("sigma_trans",z);so=interp("sigma_obs",z)
        lam=interp("lam",z);ex=interp("exit_rate",z)
        factor=fac.normal(0,float(value(p,"sigma_F",0)))
        if rho is not None:
            common=rho*common+factor*np.sqrt((1+rho)/2)
            factor=0.
        if mechanism=="log_volatility":
            log_v=-tau*tau/2+.95*(log_v+tau*tau/2)+tau*np.sqrt(1-.95**2)*vol.standard_normal(n)
            time_amp=np.exp(log_v/2)
        else: time_amp=1.
        innovation=perm.standard_normal(n)
        if mechanism=="fat_lasting":
            innovation*=np.sqrt((amount-2)/extra.chisquare(amount,n))
        if mechanism=="slow_memory":
            ks=k/10
            variance_ratio=np.divide(ks*(2-ks),k*(2-k),out=np.full(n,.1),where=k>0)
            residual=(1-k)*(level-home-slow_state)+lam*factor+sp*sqrt_w*np.sqrt(1-amount)*innovation
            slow_state=(1-ks)*slow_state+sp*sqrt_w*np.sqrt(amount*variance_ratio)*extra.standard_normal(n)
            level=home+residual+slow_state
        elif mechanism=="random_walk":
            level=level+lam*factor+amount*sp*sqrt_w*innovation
        else:
            level=home+(1-k)*(level-home)+lam*factor+sp*sqrt_w*time_amp*innovation
        passing=fast_draw()
        if mechanism=="bounded_fast": passing=np.clip(passing,-amount,amount)/bound_norm
        fast_state=phi*fast_state+st*sqrt_v*time_amp*passing
        if has_second:
            medium_state=interp("phi2",z)*medium_state+interp("sigma_trans2",z)*sqrt_v*time_amp*med.standard_normal(n)
        if mechanism=="off":
            u=off_rng.random(n);old=is_off.copy()
            is_off[old]=u[old]>=.125
            is_off[~old]=u[~old]<amount
        replace=deaths.random(n)<ex
        nr=int(replace.sum())
        if nr:
            bottom=np.asarray(value(p,"bottom_mu"))
            new=bottom[rebirth.integers(len(bottom),size=nr)]
            home[replace]=new;level[replace]=new
            fast_state[replace]=0;medium_state[replace]=0;slow_state[replace]=0
            identities[replace]=np.arange(next_identity,next_identity+nr);next_identity+=nr
            sqrt_v[replace]=amplitudes(nr);sqrt_w[replace]=lasting_amplitude(sqrt_v[replace])
            is_off[replace]=False
            if mechanism=="log_volatility":
                log_v[replace]=-tau*tau/2+tau*vol.standard_normal(nr)
                time_amp[replace]=np.exp(log_v[replace]/2)
        if tt<0: continue
        x=level+fast_state+medium_state+so*sqrt_v*time_amp*obs.standard_normal(n)
        if rho is not None: x=x+lam*common
        if mechanism=="off": x[is_off]=np.nan
        order=np.argsort(np.where(np.isfinite(x),-x,np.inf))
        observed=order[np.isfinite(x[order])]
        rank=np.zeros(n,np.int32);rank[observed]=np.arange(1,len(observed)+1)
        out["tvals"][tt]=x[selected];out["tranks"][tt]=rank[selected]
        out["tids"][tt]=identities[selected]
        out["top_ids"][tt].fill(-1)
        out["top_ids"][tt,:min(depth,len(observed))]=identities[observed[:depth]]
        out["ranksize"][tt].fill(np.nan)
        out["ranksize"][tt,:min(2000,n,len(observed))]=x[observed[:min(2000,n)]]
        if return_full:
            out["full_values"][tt]=x;out["full_ranks"][tt]=rank;out["full_ids"][tt]=identities
            out["full_latent"][tt]=level;out["full_amp"][tt]=sqrt_v
    return out
