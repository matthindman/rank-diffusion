"""Lossless deterministic packing of large V2 tables; --restore unpacks them."""
import argparse,gzip,hashlib,json,shutil
from pathlib import Path

def digest(path):
    h=hashlib.sha256()
    with Path(path).open('rb') as f:
        for b in iter(lambda:f.read(1024*1024),b''):h.update(b)
    return h.hexdigest()
def main():
    ap=argparse.ArgumentParser();ap.add_argument('--out',type=Path,required=True);ap.add_argument('--restore',action='store_true')
    a=ap.parse_args();manifest=a.out/'table_archive.json'
    if a.restore:
        for name,meta in json.loads(manifest.read_text()).items():
            if Path(name).name!=name:raise ValueError(name)
            source=a.out/(name+'.gz');target=a.out/name
            if digest(source)!=meta['gzip_sha256']:raise RuntimeError('Compressed file changed: '+name)
            if target.exists():
                if digest(target)!=meta['csv_sha256']:raise RuntimeError('Existing table differs: '+name)
                continue
            with gzip.open(source,'rb') as src,target.open('wb') as dst:shutil.copyfileobj(src,dst)
            if digest(target)!=meta['csv_sha256']:raise RuntimeError('Round-trip failure: '+name)
        return
    rows={}
    for path in sorted(a.out.glob('*.csv')):
        if path.stat().st_size<1_000_000:continue
        target=path.with_suffix('.csv.gz')
        with path.open('rb') as src,target.open('wb') as raw:
            with gzip.GzipFile(filename='',fileobj=raw,mode='wb',mtime=0) as dst:shutil.copyfileobj(src,dst)
        h=hashlib.sha256()
        with gzip.open(target,'rb') as src:
            for block in iter(lambda:src.read(1024*1024),b''):h.update(block)
        original=digest(path)
        if original!=h.hexdigest():raise RuntimeError('Round-trip failure: '+str(path))
        rows[path.name]={'csv_sha256':original,'gzip_sha256':digest(target),'csv_bytes':path.stat().st_size,'gzip_bytes':target.stat().st_size}
    manifest.write_text(json.dumps(rows,indent=2)+'\n')
    ignore=a.out/'.gitignore';s=ignore.read_text()
    for name in rows:
        if name not in s.splitlines():s+=name+'\n'
    ignore.write_text(s)
    print(json.dumps(rows,indent=2))
if __name__=='__main__':main()
