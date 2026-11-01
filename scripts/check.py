#!/usr/bin/env python3
"""Compare native results with DuckDB on deterministic adversarial inputs."""
import argparse
import hashlib
import json
from pathlib import Path
import random
import subprocess

import duckdb
import numpy as np

from prepare import ROOT, write_prep


def run(*args):
    p = subprocess.run([str(x) for x in args], text=True, capture_output=True)
    if p.returncode:
        raise RuntimeError(p.stderr[-2000:])
    return p


def canon(rows):
    return sorted(tuple('NULL' if x is None or str(x) in {'NULL', 'None'} else str(x)
                        for x in r) for r in rows)


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--queries', type=int, default=200)
    args = ap.parse_args()
    out = ROOT / 'work/check'
    out.mkdir(parents=True, exist_ok=True)
    rng = random.Random(20261101)
    g = np.asarray([rng.randrange(-1, 7) for _ in range(4099)], dtype='<i4')
    domain = [-2147483648, -1000, -11, 0, 1, 7, 99, 2147483647]
    v = np.asarray([rng.choice(domain) for _ in g], dtype='<i4')
    # One entirely NULL group and mixed valid/NULL measures in other groups.
    valid = np.asarray([x != 6 and rng.randrange(9) != 0 for x in g])
    names = [str(i) for i in range(7)]
    prep = out / 'prep'
    write_prep(prep, g, v, names, '0' * 40, True, valid)
    cube = out / 'index.ecube'
    run(ROOT / 'build/enc_cube3', 'build', prep / 'g.i32', prep / 'v.i32', cube, prep / 'v.valid')
    c = duckdb.connect()
    c.execute('SET threads=1')
    c.execute('CREATE TABLE t(g VARCHAR, v BIGINT)')
    c.executemany('INSERT INTO t VALUES (?,?)',
                  [(None if a < 0 else str(int(a)), int(b) if ok else None)
                   for a, b, ok in zip(g, v, valid)])
    queries = [([], '', ''), (['where_in=0,6'], "g IN ('0','6')", ''),
               (['where_not_in=0'], "g NOT IN ('0')", ''),
               (['having=sum>=0'], '', 'HAVING SUM(v)>=0')]
    for _ in range(args.queries):
        lo, hi = rng.choice(domain), rng.choice(domain)
        if rng.random() < .8: lo, hi = sorted((lo, hi))
        queries.append(([f'range={lo},{hi}'], f'v BETWEEN {lo} AND {hi}', ''))
    for spec, where, having in queries:
        sql = ('SELECT g,SUM(v),COUNT(*),MIN(v),MAX(v) FROM t ' +
               ('WHERE ' + where if where else '') + ' GROUP BY g ' + having)
        dst = out / 'query.json'
        run(ROOT / 'build/enc_cube3', 'query', cube, prep / 'state.dict', dst, *spec)
        assert canon(json.loads(dst.read_text())['rows']) == canon(c.execute(sql).fetchall()), spec
    # Parallel construction is byte-identical, including NULL counts.
    for threads in (1, 4):
        target = out / f'parallel{threads}.ecube'
        run(ROOT / 'build/ecube3_mt', 'build_plain_mt', prep / 'g.i32', prep / 'v.i32',
            target, threads, prep / 'v.valid', 1)
        assert sha(target) == sha(cube), threads
    # Encoded and plain builds produce the same index over the sparse value domain.
    encoded = out / 'table.encoded'
    run(ROOT / 'build/encode', 'build_round3', prep, encoded)
    from_encoded = out / 'encoded.ecube'
    run(ROOT / 'build/enc_cube3', 'build_enc', encoded, prep, 17, 13, from_encoded)
    assert sha(from_encoded) == sha(cube)

    # Conjunction cube: joint predicates, partial blocks and empty intersections.
    cg = np.asarray([rng.randrange(-1, 7) for _ in range(4099)], dtype='<i8')
    cv = np.asarray([rng.randrange(-999, 2001) for _ in cg], dtype='<i8')
    ca = np.asarray([rng.randrange(5) for _ in cg], dtype='<i8')
    cb = (ca + np.asarray([rng.randrange(3) for _ in cg])).astype('<i8')
    for n, a in [('g', cg), ('v', cv), ('a', ca), ('b', cb)]: a.tofile(out / f'{n}.i64')
    mec = out / 'index.mec'
    run(ROOT / 'build/mec', 'build', *(out / f'{n}.i64' for n in ['g', 'v', 'a', 'b']), mec, 512, 50_000_000)
    c.execute('CREATE TABLE m(g BIGINT,v BIGINT,a BIGINT,b BIGINT)')
    c.executemany('INSERT INTO m VALUES (?,?,?,?)',
                  [(None if a < 0 else int(a), int(b), int(d), int(e))
                   for a,b,d,e in zip(cg,cv,ca,cb)])
    for _ in range(min(args.queries, 100)):
        lo,hi = sorted((rng.randrange(-1200,2200),rng.randrange(-1200,2200)))
        a0,a1 = sorted((rng.randrange(5),rng.randrange(5)))
        b0,b1 = sorted((rng.randrange(7),rng.randrange(7)))
        p = run(ROOT / 'build/mec', 'query', mec, lo, hi, a0, a1, b0, b1)
        obj = json.loads(p.stdout)
        got = obj['rows'] if isinstance(obj, dict) else obj
        got = [[row[k] for k in ['g','sum','count','min','max']] for row in got]
        sql = f'SELECT g,SUM(v),COUNT(*),MIN(v),MAX(v) FROM m WHERE v BETWEEN {lo} AND {hi} AND a BETWEEN {a0} AND {a1} AND b BETWEEN {b0} AND {b1} GROUP BY g'
        assert canon(got) == canon(c.execute(sql).fetchall())

    # Clustered and block-summary paths operate on the same non-null measures.
    gg = np.maximum(cg, 0).astype('<i4')
    vv = cv.astype('<i4')
    gg.tofile(out / 'cg.i32'); vv.tofile(out / 'cv.i32')
    clst = out / 'index.clst'; bsum = out / 'index.bsum'
    run(ROOT / 'build/clst_tool', 'build', out / 'cg.i32', out / 'cv.i32', clst, '--with-stats')
    verification = json.loads(run(ROOT / 'build/clst_tool', 'verify', out / 'cg.i32', out / 'cv.i32', clst).stdout)
    assert verification['exact'] == 1
    run(ROOT / 'build/tiers_tool', 'bsum_build', out / 'cg.i32', out / 'cv.i32', bsum, 128)
    c.execute('CREATE TABLE h(g BIGINT,v BIGINT)')
    c.executemany('INSERT INTO h VALUES (?,?)', [(int(a),int(b)) for a,b in zip(gg,vv)])
    for lo,hi in [(-2000,3000),(-18,16),(3000,4000),(0,-1)]:
        dst = out / 'clustered.json'
        run(ROOT / 'build/clst_tool', 'query', clst, lo, hi, dst, '/dev/null', 3)
        expected = canon(c.execute(f'SELECT g,SUM(v),COUNT(*),MIN(v),MAX(v) FROM h WHERE v BETWEEN {lo} AND {hi} GROUP BY g').fetchall())
        assert canon(json.loads(dst.read_text())['rows']) == expected
        dst = out / 'summary.json'
        run(ROOT / 'build/tiers_tool', 'bsum_query', bsum, out / 'cg.i32', out / 'cv.i32', lo, hi, dst, '/dev/null', 3)
        assert canon(json.loads(dst.read_text())['rows']) == expected
    report = {'cube_queries': len(queries), 'mec_queries': min(args.queries,100),
              'clustered_queries': 4, 'summary_queries': 4, 'parallel_index_equal': True,
              'encoded_index_equal': True, 'duckdb_version': duckdb.__version__}
    (out / 'summary.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report))


if __name__ == '__main__': main()
