"""Safety checks for authorized lossless raw-file replacement."""
import hashlib
import json

import pytest
import zstandard as zstd

from scripts.data_wrangling import compress_verified_raw as compression


def test_replacement_preserves_every_byte(tmp_path):
    data = (bytes(range(256)) + b"\x00raw\r\n") * 4000
    source = tmp_path / "raw.json"
    source.write_bytes(data)
    result = compression.compress(source, replace=True)
    assert not source.exists()
    output = tmp_path / "raw.json.zst"
    assert zstd.ZstdDecompressor().decompress(output.read_bytes()) == data
    evidence = json.loads((tmp_path / "raw.json.zst.verification.json").read_text())
    assert evidence["source_removed"]
    assert evidence["decoded_sha256"] == hashlib.sha256(data).hexdigest()
    assert result["compressed_bytes"] < len(data)


def test_failed_verification_retains_original(tmp_path, monkeypatch):
    source = tmp_path / "raw"
    data = b"preserve me\n" * 1000
    source.write_bytes(data)
    def fail(*args):
        raise ValueError("injected decoded mismatch")
    monkeypatch.setattr(compression, "verify", fail)
    with pytest.raises(ValueError, match="mismatch"):
        compression.compress(source, replace=True)
    assert source.read_bytes() == data
    assert not (tmp_path / "raw.zst").exists()


def test_existing_output_is_never_overwritten(tmp_path):
    source = tmp_path / "raw"
    source.write_bytes(b"original")
    output = tmp_path / "raw.zst"
    output.write_bytes(b"existing")
    with pytest.raises(FileExistsError):
        compression.compress(source, replace=True)
    assert source.read_bytes() == b"original"
    assert output.read_bytes() == b"existing"


def test_truncated_archive_fails_full_verification(tmp_path):
    data = b"raw record\n" * 1000
    output = tmp_path / "broken.zst"
    output.write_bytes(zstd.ZstdCompressor(write_checksum=True).compress(data)[:-8])
    with pytest.raises((ValueError, zstd.ZstdError)):
        compression.verify(output, len(data), hashlib.sha256(data).hexdigest())
