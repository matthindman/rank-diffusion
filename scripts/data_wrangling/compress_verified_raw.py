"""Losslessly replace one raw file only after a complete decoded SHA-256 check.

Does not parse records. Interrupted outputs remain as .partial files for review.
Requires explicit --replace to remove the source; refuses existing outputs.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import time

import zstandard as zstd

CHUNK = 8 * 1024 * 1024


def signature(st):
    return (st.st_dev, st.st_ino, st.st_size, st.st_mtime_ns, st.st_ctime_ns)


def report(phase, count, total, started):
    print(json.dumps({"phase": phase, "bytes": count, "total": total,
                      "elapsed_seconds": round(time.monotonic() - started, 1)}),
          flush=True)


def verify(path, expected_size, expected_hash):
    digest = hashlib.sha256()
    count = 0
    started = last = time.monotonic()
    with path.open("rb") as raw, zstd.ZstdDecompressor().stream_reader(raw) as reader:
        while block := reader.read(CHUNK):
            digest.update(block)
            count += len(block)
            if time.monotonic() - last >= 30:
                report("verifying", count, expected_size, started)
                last = time.monotonic()
    if count != expected_size or digest.hexdigest() != expected_hash:
        raise ValueError("Decoded byte count or SHA-256 mismatch; source retained")
    return count, digest.hexdigest()


def compress(source, replace=False):
    source = Path(source).absolute()
    if source.is_symlink() or not source.is_file():
        raise ValueError("Source must be a regular non-symlink file")
    output = source.with_name(source.name + ".zst")
    partial = output.with_name(output.name + ".partial")
    manifest = output.with_name(output.name + ".verification.json")
    for path in (output, partial, manifest):
        if path.exists():
            raise FileExistsError(path)
    before = source.stat()
    digest = hashlib.sha256()
    count = 0
    started = last = time.monotonic()
    with source.open("rb") as reader, partial.open("xb") as raw:
        if signature(os.fstat(reader.fileno())) != signature(before):
            raise RuntimeError("Source changed before opening")
        compressor = zstd.ZstdCompressor(level=6, threads=2, write_checksum=True)
        with compressor.stream_writer(raw, size=before.st_size, closefd=False) as writer:
            while block := reader.read(CHUNK):
                digest.update(block)
                count += len(block)
                writer.write(block)
                if time.monotonic() - last >= 30:
                    report("compressing", count, before.st_size, started)
                    last = time.monotonic()
        raw.flush()
        os.fsync(raw.fileno())
        if signature(os.fstat(reader.fileno())) != signature(before):
            raise RuntimeError("Source changed while reading; source retained")
    if count != before.st_size or signature(source.stat()) != signature(before):
        raise RuntimeError("Source changed while compressing; source retained")
    verify(partial, count, digest.hexdigest())
    if signature(source.stat()) != signature(before):
        raise RuntimeError("Source changed while verifying; source retained")
    if partial.stat().st_size >= count:
        raise RuntimeError("Compression saved no space; source retained")
    # Hard-link promotion cannot overwrite an existing destination.
    os.link(partial, output)
    partial.unlink()
    result = {"source": str(source), "output": str(output), "decoded_bytes": count,
              "compressed_bytes": output.stat().st_size, "decoded_sha256": digest.hexdigest(),
              "source_stat": list(signature(before)), "zstandard_version": zstd.__version__,
              "level": 6, "frame_checksum": True, "full_decode_verified": True,
              "verified_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
              "replacement_authorized": replace, "source_removed": False}
    with manifest.open("x") as f:
        json.dump(result, f, indent=2)
        f.write("\n")
        f.flush()
        os.fsync(f.fileno())
    if replace:
        if signature(source.stat()) != signature(before):
            raise RuntimeError("Source changed before replacement; source retained")
        source.unlink()
        result["source_removed"] = True
        updated = manifest.with_name(manifest.name + ".updated")
        with updated.open("x") as f:
            json.dump(result, f, indent=2)
            f.write("\n")
            f.flush()
            os.fsync(f.fileno())
        os.replace(updated, manifest)
    print(json.dumps(result), flush=True)
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source")
    parser.add_argument("--replace", action="store_true")
    args = parser.parse_args()
    compress(args.source, args.replace)
