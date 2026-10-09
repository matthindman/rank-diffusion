#!/usr/bin/env python3
"""October 8 preservation audit; opaque bytes only, no scientific data access.

The owner requested all potentially useful WD data on SSD. Preserve remaining
CrowdTangle exports without assuming their overlap, plus unmatched backfill TSVs.
Keep Reddit raw, paired backfill TSVs and images on WD. Existing files are never
overwritten. Execution requires a previously reviewed inventory/plan directory.
"""
import argparse
import csv
import hashlib
import json
import os
import shutil
import time
from pathlib import Path

from phase2_common import file_manifest_row, read_manifest, upsert_manifest, utc_now

SCRIPT = "scripts/data_wrangling/preserve_wd_sources_20261008.py"
SSD = Path('/Volumes/T9/rank-diffusion-data')
WD = Path('/Volumes/My Passport for Mac')
CHUNK = 8 * 1024 * 1024


def digest(path):
    h = hashlib.sha256()
    with path.open('rb') as f:
        for block in iter(lambda: f.read(CHUNK), b''):
            h.update(block)
    return h.hexdigest()


def plan(folder):
    sources = json.loads((folder / 'wd_inventory.json').read_text())['files']
    existing = read_manifest(SSD / 'manifest/MANIFEST.csv')
    by_source = {v['source_paths']: v for v in existing.values()
                 if '|' not in v['source_paths']}
    paired = {Path(x['relative']).stem for x in sources
              if x['relative'].startswith('crowdtangle_backfill/')
              and x['relative'].endswith('.parquet')}
    rows = []
    for item in sources:
        p = Path(item['relative'])
        row = dict(item, destination='', action='', reason='')
        if item['path'] in by_source and by_source[item['path']]['role'].startswith('raw_small'):
            row.update(action='verify_existing', destination=by_source[item['path']]['file_path'],
                       reason='Previously copied raw source; verify against recorded source SHA-256')
        elif p.parts[0] == 'crowdtangle':
            row.update(action='copy', destination=str(SSD / 'raw_small/wd_preserved_20261008' / p),
                       reason='Preserve export/helper; overlap or supersession not proven; no analytical ingestion')
        elif p.parts[0] == 'crowdtangle_backfill' and p.suffix == '.parquet':
            row.update(action='copy', destination=str(SSD / 'raw_small/facebook' / p),
                       reason='Missing daily post parquet')
        elif p.parts[0] == 'crowdtangle_backfill' and (p.suffix == '.log' or
                (p.suffix == '.tsv' and p.stem not in paired)):
            row.update(action='copy', destination=str(SSD / 'raw_small/wd_preserved_20261008' / p),
                       reason='Backfill provenance or TSV without a same-day parquet')
        elif p.parts[0] in ('pushshift', 'reddit-uncompressed'):
            row.update(action='retain_wd', reason='Raw Reddit archive/fallback; 49+49 monthly aggregates already on SSD; raw does not fit SSD budget')
        elif p.suffix == '.tsv':
            row.update(action='retain_wd', reason='Paired daily backfill TSV; established project duplicate policy; not re-proven row-by-row')
        elif p.name.endswith('.tar.gz'):
            row.update(action='retain_wd', reason='Image archive outside ranked activity data requirements')
        else:
            row.update(action='retain_wd', reason='Drive installer, not research data')
        rows.append(row)
    (folder / 'transfer_plan.json').write_text(json.dumps(rows, indent=2))
    with (folder / 'source_disposition.csv').open('w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0])); writer.writeheader(); writer.writerows(rows)
    counts = {}
    for r in rows:
        c = counts.setdefault(r['action'], {'files': 0, 'bytes': 0})
        c['files'] += 1; c['bytes'] += r['bytes']
    print(json.dumps(counts, indent=2), flush=True)
    return rows


def execute(folder, recheck_completed=False):
    rows = json.loads((folder / 'transfer_plan.json').read_text())
    manifest = SSD / 'manifest/MANIFEST.csv'
    archive = SSD / 'manifest/drive_audit_20261008'
    archive.mkdir(exist_ok=True)
    for name in ('wd_inventory.json', 'ssd_inventory.json', 'transfer_plan.json', 'source_disposition.csv'):
        dest = archive / name
        if dest.exists():
            if digest(dest) != digest(folder / name):
                raise RuntimeError(f'Existing audit differs: {dest}')
        else:
            shutil.copyfile(folder / name, dest)
    backup = archive / 'MANIFEST.before.csv'
    if not backup.exists():
        shutil.copyfile(manifest, backup)
    current = read_manifest(manifest)
    usage = shutil.disk_usage(SSD)
    additional = sum(r['bytes'] for r in rows if r['action'] == 'copy' and not Path(r['destination']).exists())
    # Check BOTH project ceiling and whole-volume free-space floor, conservatively.
    if usage.used + additional > 600_000_000_000 or usage.free - additional < usage.total * .4:
        raise RuntimeError('Preservation would exceed 600 GB / 40% free budget')
    log = archive / 'verification.jsonl'
    done = {}
    if log.exists():
        for line in log.read_text().splitlines():
            event = json.loads(line)
            if event['status'] in ('copied_verified', 'verified_recorded_hash'):
                done[event['destination']] = event
    start = time.monotonic(); copied = 0
    with log.open('a', buffering=1) as events:
        def record(event):
            event['at_utc'] = utc_now()
            events.write(json.dumps(event) + '\n'); events.flush(); os.fsync(events.fileno())
        # Copy first so preservation begins promptly; recorded-hash verification follows.
        jobs = sorted([r for r in rows if r['action'] == 'copy'], key=lambda r:r['relative'])
        jobs += [r for r in rows if r['action'] == 'verify_existing']
        # Include all manifest aggregates/derived artifacts, not just raw source copies.
        destinations = {r['destination'] for r in jobs}
        for dest, m in current.items():
            if dest not in destinations and m['sha256']:
                jobs.append({'action':'verify_manifest', 'destination':dest,
                             'path':m['source_paths'], 'bytes':int(m['bytes'])})
        for i, r in enumerate(jobs, 1):
            dst = Path(r['destination'])
            if not dst.is_relative_to(SSD):
                raise RuntimeError(f'Destination outside SSD root: {dst}')
            if dst.exists() and str(dst) in done:
                prev = done[str(dst)]
                if dst.stat().st_size == prev['bytes'] and dst.stat().st_mtime_ns == prev['destination_mtime_ns']:
                    if recheck_completed:
                        if digest(dst) != prev['sha256']:
                            raise RuntimeError(f'Completed file failed reconnect recheck: {dst}')
                        print(f'[{i}/{len(jobs)}] reconnect hash verified: {dst.name}', flush=True)
                    continue
            print(f'[{i}/{len(jobs)}] {r["action"]} {dst.name} {r["bytes"]/1e9:.3f} GB', flush=True)
            if r['action'] == 'copy':
                src = Path(r['path'])
                if not src.is_relative_to(WD):
                    raise RuntimeError('Source outside WD')
                before = src.stat()
                if before.st_size != r['bytes'] or before.st_mtime_ns != r['mtime_ns']:
                    raise RuntimeError(f'Source changed since inventory: {src}')
                dst.parent.mkdir(parents=True, exist_ok=True)
                if dst.exists():
                    source_sha, dest_sha = digest(src), digest(dst)
                    if source_sha != dest_sha:
                        raise RuntimeError(f'Existing destination conflict, preserved: {dst}')
                else:
                    tmp = dst.with_name(dst.name + '.preservation-staging')
                    h = hashlib.sha256()
                    # Exclusive create deliberately refuses leftovers for inspection.
                    with src.open('rb') as f, tmp.open('xb') as out:
                        for block in iter(lambda:f.read(CHUNK), b''):
                            out.write(block); h.update(block)
                        out.flush(); os.fsync(out.fileno())
                    source_sha = h.hexdigest(); dest_sha = digest(tmp)
                    after = src.stat()
                    if (after.st_size, after.st_mtime_ns) != (before.st_size, before.st_mtime_ns):
                        raise RuntimeError('Source changed during copy')
                    if source_sha != dest_sha or tmp.stat().st_size != before.st_size:
                        raise RuntimeError(f'Copy verification failed, staging retained: {tmp}')
                    if dst.exists():
                        raise RuntimeError(f'Destination appeared during copy: {dst}')
                    tmp.rename(dst)
                upsert_manifest(manifest, [file_manifest_row(dst, role='raw_small/preservation_20261008',
                    sha256=dest_sha, source_paths=str(src), source_bytes=before.st_size,
                    source_sha256=source_sha, script=SCRIPT, parameters='opaque-byte copy; fsync; destination reread; no overwrite',
                    status='copied_verified', notes=r['reason'])])
                copied += before.st_size
                record({'status':'copied_verified', 'source':str(src), 'destination':str(dst),
                        'bytes':before.st_size, 'sha256':dest_sha, 'source_sha256':source_sha,
                        'destination_mtime_ns':dst.stat().st_mtime_ns})
            else:
                expected = current[str(dst)]
                actual = digest(dst)
                if dst.stat().st_size != int(expected['bytes']) or actual != expected['sha256']:
                    record({'status':'HASH_MISMATCH', 'destination':str(dst), 'actual_sha256':actual,
                            'expected_sha256':expected['sha256']})
                    raise RuntimeError(f'Recorded hash mismatch; file preserved: {dst}')
                record({'status':'verified_recorded_hash','destination':str(dst),'bytes':dst.stat().st_size,
                        'sha256':actual,'destination_mtime_ns':dst.stat().st_mtime_ns})
            if i % 25 == 0:
                print(f'Progress: {i}/{len(jobs)}; newly copied {copied/1e9:.2f} GB; elapsed {(time.monotonic()-start)/60:.1f} min', flush=True)
    print('COMPLETE: copy and manifest verification', flush=True)


if __name__ == '__main__':
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('inventory_dir', type=Path)
    ap.add_argument('--execute', action='store_true')
    ap.add_argument('--recheck-completed', action='store_true',
                    help='Rehash previously completed files, e.g. after a cable disconnect')
    args = ap.parse_args()
    execute(args.inventory_dir, args.recheck_completed) if args.execute else plan(args.inventory_dir)
