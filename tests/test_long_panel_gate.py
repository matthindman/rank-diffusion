"""A1.1 fail-closed gate for the submissions long panels: the good
construction PASSES; every registered failure mode FAILS (PREREG
2026-07-16 A1.1/A1.11 adversarial requirement)."""
import sys
import unittest
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "llm_fitting"))
import check_long_panels as clp  # noqa: E402

TMP = Path("/tmp/long_panel_gate_test")
FIRST_W, LAST_W = "2019-01-07", "2019-03-25"   # 12 complete weeks
MONTHS = ["2019-01", "2019-02", "2019-03"]


def build_fixture(tmp=TMP):
    import shutil
    shutil.rmtree(tmp, ignore_errors=True)
    tmp.mkdir(parents=True)
    rng = np.random.default_rng(0)
    ids = [f"s{i:03d}" for i in range(25)]
    days = pd.date_range("2019-01-04", "2019-03-28", freq="D")  # boundary days both ends
    rows = []
    for d in days:
        for e in ids:
            sk = int(rng.integers(-5, 60))
            ck = int(rng.integers(-5, 40))
            rows.append((d, e, max(sk, 0), sk, ck,
                         int(rng.integers(0, 9)), int(rng.integers(0, 9))))
    daily = pd.DataFrame(rows, columns=clp.COLS[1:2] + clp.COLS[0:1] + clp.COLS[2:])
    daily = daily[["date", "endpoint_id"] + clp.COLS[2:]]
    wk = daily["date"] - pd.to_timedelta(daily["date"].dt.weekday, unit="D")
    complete = pd.date_range(FIRST_W, LAST_W, freq="7D")
    weekly = (daily.assign(date=wk).groupby(["endpoint_id", "date"],
                                            as_index=False)[clp.COLS[2:]].sum())
    weekly = weekly[weekly["date"].isin(complete)]
    log = pd.DataFrame({
        "source": "x", "record_type": ["submissions"] * len(MONTHS),
        "month": MONTHS, "status": "ok", "ok_flag": 1,
        "lines": 1000, "errors": 0, "error_rate": 0.0, "rows": 10,
        "out_dir": "y"})
    d_p, w_p, l_p = tmp / "d.parquet", tmp / "w.parquet", tmp / "log.csv"
    daily.to_parquet(d_p, index=False)
    weekly.to_parquet(w_p, index=False)
    log.to_csv(l_p, index=False)
    return daily, weekly, log, str(d_p), str(w_p), str(l_p)


def run_gate(d_p, w_p, l_p):
    clp.check(d_p, w_p, l_p, FIRST_W, LAST_W, MONTHS, "submissions")


class LongPanelGateTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        (cls.daily, cls.weekly, cls.log,
         cls.d_p, cls.w_p, cls.l_p) = build_fixture()

    def _rw(self, df, name):
        p = str(TMP / name)
        df.to_parquet(p, index=False)
        return p

    def test_good_construction_passes(self):
        run_gate(self.d_p, self.w_p, self.l_p)

    def test_failure_modes_fail(self):
        d, w = self.daily, self.weekly
        # (name, daily-variant, weekly-variant, log-variant)
        ghost = pd.DataFrame([[pd.Timestamp("2019-02-05"), "ghost",
                               5, 5, 0, 1, 0]], columns=d.columns)
        broken_id = d.copy()
        i = broken_id.index[broken_id["submission_karma"] < 0][0]
        broken_id.loc[i, "metric_value"] = 1
        neg = d.copy(); neg.loc[neg.index[0], "comment_count"] = -1
        nul = d.copy(); nul["comment_karma"] = nul["comment_karma"].astype(float)
        nul.loc[nul.index[0], "comment_karma"] = np.nan
        frac = d.copy(); frac["submission_karma"] = frac["submission_karma"].astype(float)
        frac.loc[frac.index[0], "submission_karma"] = 2.5
        extra = d.assign(bonus=1)
        gap_day = d[d["date"] != pd.Timestamp("2019-02-14")]
        dup = pd.concat([d, d.iloc[[0]]], ignore_index=True)
        w_hole = w[w["date"] != pd.Timestamp("2019-02-11")]      # non-consecutive
        w_extra = pd.concat([w, pd.DataFrame(
            [["zzz", pd.Timestamp("2019-02-11"), 5, 5, 0, 1, 0]],
            columns=w.columns)], ignore_index=True)              # weekly-only cell
        w_val = w.copy(); w_val.loc[w_val.index[5], "metric_value"] += 3
        log_err = self.log.copy(); log_err.loc[1, "errors"] = 9
        log_missing = self.log[self.log["month"] != "2019-02"]
        cases = [
            ("ghost daily-only cell", pd.concat([d, ghost]), w, self.log),
            ("broken daily identity", broken_id, w, self.log),
            ("negative count", neg, w, self.log),
            ("null value", nul, w, self.log),
            ("non-integral", frac, w, self.log),
            ("unregistered column", extra, w, self.log),
            ("missing calendar day", gap_day, w, self.log),
            ("duplicate key", dup, w, self.log),
            ("non-consecutive weeks", d, w_hole, self.log),
            ("weekly-only cell", d, w_extra, self.log),
            ("value mismatch", d, w_val, self.log),
            ("errors>0", d, w, log_err),
            ("missing month", d, w, log_missing),
        ]
        for name, dd, ww, ll in cases:
            with self.subTest(name):
                d_p = self._rw(dd, "bad_d.parquet")
                w_p = self._rw(ww, "bad_w.parquet")
                l_p = str(TMP / "bad_log.csv"); ll.to_csv(l_p, index=False)
                with self.assertRaises(SystemExit, msg=name):
                    run_gate(d_p, w_p, l_p)

    def test_day_guard_collapse_fails(self):
        # 90% row collapse on one day, weekly rebuilt consistently -- only
        # the guard can catch it
        d = self.daily
        bad_day = pd.Timestamp("2019-03-06")
        keep = ~((d["date"] == bad_day)
                 & (d["endpoint_id"] != "s000") & (d["endpoint_id"] != "s001"))
        d2 = d[keep]
        wk = d2["date"] - pd.to_timedelta(d2["date"].dt.weekday, unit="D")
        complete = pd.date_range(FIRST_W, LAST_W, freq="7D")
        w2 = (d2.assign(date=wk).groupby(["endpoint_id", "date"],
                                         as_index=False)[clp.COLS[2:]].sum())
        w2 = w2[w2["date"].isin(complete)]
        with self.assertRaises(SystemExit):
            run_gate(self._rw(d2, "cg_d.parquet"),
                     self._rw(w2, "cg_w.parquet"), self.l_p)


if __name__ == "__main__":
    unittest.main()
