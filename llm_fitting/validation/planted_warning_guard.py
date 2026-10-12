"""Opt-in warning-capture guard for V2; changes metadata work, never statistics.

The original runner counts each captured warning with a nested scan. NumPy
can emit one identical all-NaN warning per identity column. Capture each
message once in a call instead, and stamp this execution wrapper in every
checkpoint. Original sources and their identities remain unchanged.
"""
from contextlib import contextmanager
from pathlib import Path
import os,sys,warnings

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/"llm_fitting"))
from validation import planted_crosscheck as pc

@contextmanager
def bounded_warnings():
    original_filter=warnings.simplefilter
    original_write=pc.write
    meta=dict(path="llm_fitting/validation/planted_warning_guard.py",
              sha256=pc.digest(__file__),warning_capture="once, not occurrence counts")
    def simplefilter(action,*args,**kwargs):
        return original_filter("once" if action=="always" else action,*args,**kwargs)
    def write(path,data):
        path=Path(path)
        if path.parent.name=="attempts" and isinstance(data,dict):
            # time_ns has finite clock resolution; concurrent workers can collide.
            tag=f"{data.get('stage','event')}_{data.get('panel','all')}_{os.getpid()}"
            path=path.with_name(path.stem+"_"+tag+path.suffix)
        if isinstance(data,dict):data={**data,"execution_wrapper":meta}
        return original_write(path,data)
    warnings.simplefilter=simplefilter;pc.write=write
    try:yield
    finally:
        warnings.simplefilter=original_filter;pc.write=original_write
def main():
    if len(sys.argv)<2 or sys.argv[1] not in ("study","extension"):
        raise SystemExit("Usage: planted_warning_guard.py {study|extension} [runner arguments]")
    mode=sys.argv.pop(1)
    if mode=="study":
        from validation import planted_study as runner
    else:
        from validation import planted_extension as runner
    if mode=="study" and len(sys.argv)>1 and sys.argv[1]=="diagnostics":
        # Fresh diagnostic workers use the verified exact eligibility reduction.
        # Existing workers keep their loaded code and cached producer hash.
        from validation.planted_efficient_run import eligible_statistics
        with bounded_warnings(),eligible_statistics():runner.main()
    else:
        with bounded_warnings():runner.main()
if __name__=="__main__":main()
