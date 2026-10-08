import gzip
import runpy
from pathlib import Path

import pyarrow.parquet as pq


MODULE = runpy.run_path(
    str(Path(__file__).parents[1] / "scripts" / "data_wrangling" / "wikipedia_pageviews_pilot.py")
)


def test_ends_aware_extractor_preserves_title_spaces(tmp_path):
    source = tmp_path / "pageviews-20250101-000000.gz"
    output = tmp_path / "hour.parquet"
    with gzip.open(source, "wb") as fh:
        fh.write(b"en Apollo Theater 3 0\n")
        fh.write(b"en.m Apollo Theater 5 0\n")
        fh.write(b"fr Apollo Theater 11 0\n")

    meta = MODULE["extract_hour"](source, output, 0, batch_size=1)
    table = pq.read_table(output).to_pydict()

    assert meta["rows"] == 2
    assert meta["malformed"] == 0
    assert table["endpoint_id"] == ["Apollo Theater", "Apollo Theater"]
    assert table["access"] == ["desktop", "mobile"]
    assert table["count_views"] == [3, 5]


def test_gzip_integrity_check_rejects_corruption(tmp_path):
    source = tmp_path / "bad.gz"
    source.write_bytes(b"not a gzip stream")
    valid, message = MODULE["gzip_ok"](source)
    assert not valid
    assert message
