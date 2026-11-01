#!/usr/bin/env python3
"""P1: one generic encoded evaluator executes a set of new queries over four encodings.

For each spec, the generic_eval binary (no per-query adapter) evaluates the encoded tree;
the expected result comes from DuckDB on the persisted projected/full database. The same
canonicalization (string group, decimal values, lexicographic order) is applied to both.
"""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parents[1]))
import artifact_paths as _artifact
import argparse
import json
import statistics
import subprocess
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
EVAL = ROOT / 'src_encoded_ops_20261030/generic_eval'
OUT = ROOT / 'results/p1_generic_eval_20261030'
OUT.mkdir(parents=True, exist_ok=True)


def sql_rows(db, sql):
    import duckdb
    con = duckdb.connect(str(db), read_only=True, config={'threads': '1'})
    rows = con.execute(sql).fetchall()
    con.close()
    return [[str(v) if v is not None else None for v in row] for row in rows]


def canonical(rows):
    return sorted(([str(v) for v in row] for row in rows), key=lambda r: r[0].encode())


def build_specs():
    ig2 = {
        'encoded': 'results/ig2_prep_20261025/Observed.encoded',
        'prep': 'results/ig2_prep_20261025/prepared/Observed',
        'dict': 'state.dict', 'group_id': 17,
    }
    h8 = {'encoded': 'results/hatred8_prep_v5/Observed.encoded',
          'prep': 'results/hatred8_prep_v5/Observed', 'dict': 'col24.dict', 'group_id': 24}
    h10 = {'encoded': 'results/hatred10_prep_v1/Observed.encoded',
           'prep': 'results/hatred10_prep_v1/Observed', 'dict': 'col24.dict', 'group_id': 24}
    cmc = {'encoded': 'results/citymax14_prep_v3/Observed.encoded',
           'prep': 'results/citymax14_prep_v3/Observed', 'dict': 'col2.dict', 'group_id': 2}
    specs = []
    def add(name, base, aggs, where_in=None, where_not_in=None, sql=None, db=None):
        s = dict(base); s['aggs'] = aggs
        if where_in is not None: s['where_in'] = where_in
        if where_not_in is not None: s['where_not_in'] = where_not_in
        specs.append({'name': name, 'spec': s, 'sql': sql, 'db': db})
    igdb = 'results/run_ig2_proj2_v3/Observed_proj2/projected.duckdb'
    t = '"IGlocations1_1"'
    add('ig2_sum_pop', ig2, [{'op': 'sum', 'id': 13, 'bias': 2147483648}],
        where_not_in=['District of Columbia'],
        sql=f'SELECT "State", SUM(CAST("POPESTIMATE2014" AS BIGINT)) FROM {t} WHERE "State" NOT IN (\'District of Columbia\') GROUP BY 1',
        db=igdb)
    add('ig2_count', ig2, [{'op': 'count'}], where_not_in=['District of Columbia'],
        sql=f'SELECT "State", COUNT(*) FROM {t} WHERE "State" NOT IN (\'District of Columbia\') GROUP BY 1', db=igdb)
    add('ig2_min_pop', ig2, [{'op': 'min', 'id': 13, 'bias': 2147483648}],
        where_not_in=['District of Columbia'],
        sql=f'SELECT "State", MIN("POPESTIMATE2014") FROM {t} WHERE "State" NOT IN (\'District of Columbia\') GROUP BY 1', db=igdb)
    add('ig2_max_pop', ig2, [{'op': 'max', 'id': 13, 'bias': 2147483648}],
        where_not_in=['District of Columbia'],
        sql=f'SELECT "State", MAX("POPESTIMATE2014") FROM {t} WHERE "State" NOT IN (\'District of Columbia\') GROUP BY 1', db=igdb)
    add('ig2_sum_west', ig2, [{'op': 'sum', 'id': 13, 'bias': 2147483648}],
        where_in=['California', 'Texas', 'New York'],
        sql=f'SELECT "State", SUM(CAST("POPESTIMATE2014" AS BIGINT)) FROM {t} WHERE "State" IN (\'California\',\'Texas\',\'New York\') GROUP BY 1',
        db=igdb)
    h8db = 'results/hatred8_prep_v5/Observed_duckdb/full31.duckdb'
    add('h8_count', h8, [{'op': 'count'}],
        sql='SELECT "State ", COUNT(*) FROM "Hatred_1" GROUP BY 1', db=h8db)
    add('h8_count_states', h8, [{'op': 'count'}], where_in=['AK', 'CA', 'TX', 'NY'],
        sql='SELECT "State ", COUNT(*) FROM "Hatred_1" WHERE "State " IN (\'AK\',\'CA\',\'TX\',\'NY\') GROUP BY 1', db=h8db)
    h10db = 'results/hatred10_prep_v1/Observed_duckdb/full31.duckdb'
    add('h10_sum_pop', h10, [{'op': 'sum', 'id': 26, 'bias': 2147483648}],
        sql='SELECT "State ", SUM("StatePopNum") FROM "Hatred_1" GROUP BY 1', db=h10db)
    add('h10_max_pop', h10, [{'op': 'max', 'id': 26, 'bias': 2147483648}],
        sql='SELECT "State ", MAX("StatePopNum") FROM "Hatred_1" GROUP BY 1', db=h10db)
    add('h10_min_pop', h10, [{'op': 'min', 'id': 26, 'bias': 2147483648}],
        sql='SELECT "State ", MIN("StatePopNum") FROM "Hatred_1" GROUP BY 1', db=h10db)
    add('h10_count', h10, [{'op': 'count'}],
        sql='SELECT "State ", COUNT(*) FROM "Hatred_1" GROUP BY 1', db=h10db)
    # Expanded grid: IG/2 aggregate x filter variants.
    filter_variants = [
        ('dc', None, ['District of Columbia'], '\"State\" NOT IN (\'District of Columbia\')'),
        ('west', ['California', 'Texas', 'New York'], None, '\"State\" IN (\'California\',\'Texas\',\'New York\')'),
        ('south', ['Florida', 'Georgia', 'Alabama'], None, '\"State\" IN (\'Florida\',\'Georgia\',\'Alabama\')'),
        ('northeast', ['New York', 'Massachusetts', 'New Jersey'], None, '\"State\" IN (\'New York\',\'Massachusetts\',\'New Jersey\')'),
    ]
    for fname, win, wnot, where_sql in filter_variants:
        for op, expr in [('sum', 'SUM(CAST(\"POPESTIMATE2014\" AS BIGINT))'),
                         ('min', 'MIN(\"POPESTIMATE2014\")'),
                         ('max', 'MAX(\"POPESTIMATE2014\")'),
                         ('count', 'COUNT(*)')]:
            add(f'ig2_{op}_{fname}', ig2,
                [{'op': op, 'id': 13, 'bias': 2147483648}] if op != 'count' else [{'op': 'count'}],
                where_in=win, where_not_in=wnot,
                sql=f'SELECT \"State\", {expr} FROM {t} WHERE {where_sql} GROUP BY 1', db=igdb)
    h8db = 'results/hatred8_prep_v5/Observed_duckdb/full31.duckdb'
    h8kw = dict(h8); h8kw['dict'] = 'col13.dict'; h8kw['group_id'] = 13
    add('h8_keyword_count', h8kw, [{'op': 'count'}],
        sql='SELECT \"Keyword\", COUNT(*) FROM \"Hatred_1\" GROUP BY 1', db=h8db)
    h10kw = dict(h10); h10kw['dict'] = 'col13.dict'; h10kw['group_id'] = 13
    add('h10_keyword_count', h10kw, [{'op': 'count'}],
        sql='SELECT \"Keyword\", COUNT(*) FROM \"Hatred_1\" GROUP BY 1', db='results/hatred10_prep_v1/Observed_duckdb/full31.duckdb')
    # Multi-aggregate specifications share one scan.
    add('ig2_sum_count_dc', ig2,
        [{'op': 'sum', 'id': 13, 'bias': 2147483648}, {'op': 'count'}],
        where_not_in=['District of Columbia'],
        sql=f'SELECT \"State\", SUM(CAST(\"POPESTIMATE2014\" AS BIGINT)), COUNT(*) FROM {t} WHERE \"State\" NOT IN (\'District of Columbia\') GROUP BY 1',
        db=igdb)
    add('ig2_min_max_dc', ig2,
        [{'op': 'min', 'id': 13, 'bias': 2147483648}, {'op': 'max', 'id': 13, 'bias': 2147483648}],
        where_not_in=['District of Columbia'],
        sql=f'SELECT \"State\", MIN(\"POPESTIMATE2014\"), MAX(\"POPESTIMATE2014\") FROM {t} WHERE \"State\" NOT IN (\'District of Columbia\') GROUP BY 1',
        db=igdb)
    add('ig2_sum_min_max_dc', ig2,
        [{'op': 'sum', 'id': 13, 'bias': 2147483648},
         {'op': 'min', 'id': 13, 'bias': 2147483648},
         {'op': 'max', 'id': 13, 'bias': 2147483648}],
        where_not_in=['District of Columbia'],
        sql=f'SELECT \"State\", SUM(CAST(\"POPESTIMATE2014\" AS BIGINT)), MIN(\"POPESTIMATE2014\"), MAX(\"POPESTIMATE2014\") FROM {t} WHERE \"State\" NOT IN (\'District of Columbia\') GROUP BY 1',
        db=igdb)
    h10db = 'results/hatred10_prep_v1/Observed_duckdb/full31.duckdb'
    add('h10_sum_count', h10,
        [{'op': 'sum', 'id': 26, 'bias': 2147483648}, {'op': 'count'}],
        sql='SELECT \"State \", SUM(\"StatePopNum\"), COUNT(*) FROM \"Hatred_1\" GROUP BY 1', db=h10db)
    add('h10_min_max', h10,
        [{'op': 'min', 'id': 26, 'bias': 2147483648}, {'op': 'max', 'id': 26, 'bias': 2147483648}],
        sql='SELECT \"State \", MIN(\"StatePopNum\"), MAX(\"StatePopNum\") FROM \"Hatred_1\" GROUP BY 1', db=h10db)
    # HAVING specifications: output-cardinality reduction on the encoded path.
    add('ig2_sum_having', ig2, [{'op': 'sum', 'id': 13, 'bias': 2147483648,
                                 'having': {'min': 30000000}}],
        where_not_in=['District of Columbia'],
        sql=f'SELECT \"State\", SUM(CAST(\"POPESTIMATE2014\" AS BIGINT)) FROM {t} WHERE \"State\" NOT IN (\'District of Columbia\') GROUP BY 1 HAVING SUM(CAST(\"POPESTIMATE2014\" AS BIGINT)) >= 30000000',
        db=igdb)
    add('ig2_count_having', ig2, [{'op': 'count', 'having': {'min': 1000}}],
        where_not_in=['District of Columbia'],
        sql=f'SELECT \"State\", COUNT(*) FROM {t} WHERE \"State\" NOT IN (\'District of Columbia\') GROUP BY 1 HAVING COUNT(*) >= 1000',
        db=igdb)
    add('h10_count_having', h10, [{'op': 'count', 'having': {'min': 30}}],
        sql='SELECT \"State \", COUNT(*) FROM \"Hatred_1\" GROUP BY 1 HAVING COUNT(*) >= 30', db='results/hatred10_prep_v1/Observed_duckdb/full31.duckdb')
    add('cmc_count_having_1000', cmc, [{'op': 'count', 'having': {'min': 1000}}],
        sql='SELECT \"City\", COUNT(*) FROM \"CityMaxCapita_1\" WHERE \"City\" IS NOT NULL GROUP BY 1 HAVING COUNT(*) >= 1000',
        db='results/citymax14_prep_v3/Observed_duckdb/full31.duckdb')
    add('cmc_count_having_100', cmc, [{'op': 'count', 'having': {'min': 100}}],
        sql='SELECT \"City\", COUNT(*) FROM \"CityMaxCapita_1\" WHERE \"City\" IS NOT NULL GROUP BY 1 HAVING COUNT(*) >= 100',
        db='results/citymax14_prep_v3/Observed_duckdb/full31.duckdb')
    # Non-group encoded filters: keyword column predicate before grouping.
    h8db2 = 'results/hatred8_prep_v5/Observed_duckdb/full31.duckdb'
    add('h8_count_keyword_filter', h8, [{'op': 'count'}],
        sql='SELECT "State ", COUNT(*) FROM "Hatred_1" WHERE "Keyword" IN (\'bimbo\',\'hag\',\'twat\') GROUP BY 1',
        db=h8db2)
    # attach the filter to the generated spec
    specs[-1]['spec']['filters'] = [{'id': 13, 'dict': 'col13.dict', 'values': ['bimbo', 'hag', 'twat']}]
    h10db2 = 'results/hatred10_prep_v1/Observed_duckdb/full31.duckdb'
    add('h10_sum_keyword_filter', h10, [{'op': 'sum', 'id': 26, 'bias': 2147483648}],
        sql='SELECT "State ", SUM("StatePopNum") FROM "Hatred_1" WHERE "Keyword" IN (\'fatass\',\'fatso\',\'fattie\') GROUP BY 1',
        db=h10db2)
    specs[-1]['spec']['filters'] = [{'id': 13, 'dict': 'col13.dict', 'values': ['fatass', 'fatso', 'fattie']}]
    # Fifth dataset: IG/16 (Public BI) city grouping with HAVING to keep output small.
    ig16 = {'encoded': 'results/ig16_prep_v1/Observed.encoded',
            'prep': 'results/ig16_prep_v1/Observed', 'dict': 'city.dict', 'group_id': 3}
    add('ig16_city_count_having', ig16, [{'op': 'count', 'having': {'min': 30}}],
        sql='SELECT "City", COUNT(*) FROM "IGlocations1_1" GROUP BY 1 HAVING COUNT(*) >= 30',
        db='results/ig16_controls/city.duckdb')
    # ---- expanded grid: more IG/2 filters x aggregates ----
    more_filters = [
        ('west2', ['California', 'Oregon', 'Washington']),
        ('northeast2', ['Maine', 'New Hampshire', 'Vermont', 'Massachusetts', 'Rhode Island', 'Connecticut']),
        ('south2', ['Texas', 'Florida', 'Georgia', 'Alabama', 'Mississippi', 'Louisiana']),
        ('midwest', ['Illinois', 'Ohio', 'Michigan', 'Indiana', 'Wisconsin', 'Minnesota', 'Missouri', 'Iowa']),
        ('mountain', ['Colorado', 'Arizona', 'New Mexico', 'Utah', 'Nevada', 'Idaho', 'Montana', 'Wyoming']),
        ('pacific', ['Alaska', 'Hawaii', 'California', 'Oregon', 'Washington']),
        ('east', ['New York', 'New Jersey', 'Pennsylvania', 'Massachusetts']),
    ]
    for fname, states in more_filters:
        inlist = ','.join("\'" + x + "\'" for x in states)
        for op, expr in [('sum', 'SUM(CAST(\"POPESTIMATE2014\" AS BIGINT))'),
                         ('min', 'MIN(\"POPESTIMATE2014\")'),
                         ('max', 'MAX(\"POPESTIMATE2014\")'),
                         ('count', 'COUNT(*)')]:
            add(f'ig2_{op}_{fname}', ig2,
                [{'op': op, 'id': 13, 'bias': 2147483648}] if op != 'count' else [{'op': 'count'}],
                where_in=states,
                sql=f'SELECT \"State\", {expr} FROM {t} WHERE \"State\" IN ({inlist}) GROUP BY 1', db=igdb)
    # ---- expanded H10 keyword-filter aggregates ----
    kw_sets = [('bimbo', ['bimbo', 'hag', 'twat']), ('fat', ['fatass', 'fatso', 'fattie']),
               ('slur', ['slut', 'whore', 'cunt'])]
    for kname, kws in kw_sets:
        inlist = ','.join("\'" + k + "\'" for k in kws)
        for op, expr in [('sum', 'SUM(\"StatePopNum\")'), ('count', 'COUNT(*)'),
                         ('min', 'MIN(\"StatePopNum\")'), ('max', 'MAX(\"StatePopNum\")')]:
            add(f'h10_{op}_kw_{kname}', h10,
                [{'op': op, 'id': 26, 'bias': 2147483648}] if op != 'count' else [{'op': 'count'}],
                sql=f'SELECT \"State \", {expr} FROM \"Hatred_1\" WHERE \"Keyword\" IN ({inlist}) GROUP BY 1',
                db='results/hatred10_prep_v1/Observed_duckdb/full31.duckdb')
            specs[-1]['spec']['filters'] = [{'id': 13, 'dict': 'col13.dict', 'values': kws}]
    # ---- expanded H8 keyword-filter counts ----
    h8db3 = 'results/hatred8_prep_v5/Observed_duckdb/full31.duckdb'
    for kname, kws in kw_sets:
        inlist = ','.join("\'" + k + "\'" for k in kws)
        add(f'h8_count_kw_{kname}', h8, [{'op': 'count'}],
            sql=f'SELECT \"State \", COUNT(*) FROM \"Hatred_1\" WHERE \"Keyword\" IN ({inlist}) GROUP BY 1',
            db=h8db3)
        specs[-1]['spec']['filters'] = [{'id': 13, 'dict': 'col13.dict', 'values': kws}]
    # ---- CMC and IG/16 HAVING thresholds ----
    for th in (1, 5, 10, 50, 200, 500):
        add(f'cmc_count_having_{th}', cmc, [{'op': 'count', 'having': {'min': th}}],
            sql=f'SELECT \"City\", COUNT(*) FROM \"CityMaxCapita_1\" WHERE \"City\" IS NOT NULL GROUP BY 1 HAVING COUNT(*) >= {th}',
            db='results/citymax14_prep_v3/Observed_duckdb/full31.duckdb')
    for th in (1, 5, 10, 20, 50):
        add(f'ig16_city_count_having_{th}', ig16, [{'op': 'count', 'having': {'min': th}}],
            sql=f'SELECT \"City\", COUNT(*) FROM \"IGlocations1_1\" GROUP BY 1 HAVING COUNT(*) >= {th}',
            db='results/ig16_controls/city.duckdb')
    # ---- more multi-aggregate specs ----
    add('ig2_sum_count_west2', ig2,
        [{'op': 'sum', 'id': 13, 'bias': 2147483648}, {'op': 'count'}],
        where_in=['California', 'Oregon', 'Washington'],
        sql=f'SELECT \"State\", SUM(CAST(\"POPESTIMATE2014\" AS BIGINT)), COUNT(*) FROM {t} WHERE \"State\" IN (\'California\',\'Oregon\',\'Washington\') GROUP BY 1', db=igdb)
    add('h10_sum_count_kw_bimbo', h10,
        [{'op': 'sum', 'id': 26, 'bias': 2147483648}, {'op': 'count'}],
        sql='SELECT \"State \", SUM(\"StatePopNum\"), COUNT(*) FROM \"Hatred_1\" WHERE \"Keyword\" IN (\'bimbo\',\'hag\',\'twat\') GROUP BY 1',
        db='results/hatred10_prep_v1/Observed_duckdb/full31.duckdb')
    specs[-1]['spec']['filters'] = [{'id': 13, 'dict': 'col13.dict', 'values': ['bimbo', 'hag', 'twat']}]
    # Real-world 12M NYC taxi and generated 12M TPC-H SF2 specs in the same grid.
    nyc = {'encoded': 'results/nyc_taxi_2023_prep/Observed.encoded',
           'prep': 'results/nyc_taxi_2023_prep/Observed', 'dict': 'state.dict', 'group_id': 17}
    nycdb = 'results/nyc_taxi_2023_duckdb/nyc.duckdb'
    for op, expr in [('sum', 'SUM(fare_cents)'), ('count', 'COUNT(*)'),
                     ('min', 'MIN(fare_cents)'), ('max', 'MAX(fare_cents)')]:
        add(f'nyc_{op}', nyc,
            [{'op': op, 'id': 13, 'bias': 2147483648}] if op != 'count' else [{'op': 'count'}],
            sql=f'SELECT passenger_count, {expr} FROM nyc WHERE passenger_count IS NOT NULL GROUP BY 1',
            db=nycdb)
    add('nyc_sum_having', nyc, [{'op': 'sum', 'id': 13, 'bias': 2147483648,
                                'having': {'min': 5000000000}}],
        sql='SELECT passenger_count, SUM(fare_cents) FROM nyc WHERE passenger_count IS NOT NULL GROUP BY 1 HAVING SUM(fare_cents) >= 5000000000',
        db=nycdb)
    add('nyc_count_having', nyc, [{'op': 'count', 'having': {'min': 1000000}}],
        sql='SELECT passenger_count, COUNT(*) FROM nyc WHERE passenger_count IS NOT NULL GROUP BY 1 HAVING COUNT(*) >= 1000000',
        db=nycdb)
    add('nyc_min_having', nyc, [{'op': 'min', 'id': 13, 'bias': 2147483648,
                                'having': {'min': -50000}}],
        sql='SELECT passenger_count, MIN(fare_cents) FROM nyc WHERE passenger_count IS NOT NULL GROUP BY 1 HAVING MIN(fare_cents) >= -50000',
        db=nycdb)
    nyc100 = {'encoded': 'results/nyc_taxi_100m_prep/Observed.encoded',
              'prep': 'results/nyc_taxi_100m_prep/Observed', 'dict': 'state.dict', 'group_id': 17}
    nyc100db = 'results/nyc_taxi_100m_duckdb/nyc.duckdb'
    for op, expr in [('sum', 'SUM(fare_cents)'), ('count', 'COUNT(*)'),
                     ('min', 'MIN(fare_cents)'), ('max', 'MAX(fare_cents)')]:
        add(f'nyc100_{op}', nyc100,
            [{'op': op, 'id': 13, 'bias': 2147483648}] if op != 'count' else [{'op': 'count'}],
            sql=f'SELECT passenger_count, {expr} FROM nyc WHERE passenger_count IS NOT NULL GROUP BY 1',
            db=nyc100db)
    tpch = {'encoded': 'results/tpch_sf2_prep/Observed.encoded',
            'prep': 'results/tpch_sf2_prep/Observed', 'dict': 'state.dict', 'group_id': 17}
    tpchdb = 'results/tpch_sf2_lineitem_duckdb/lineitem.duckdb'
    for op, expr in [('sum', 'SUM(CAST(l_extendedprice * 100 AS BIGINT))'), ('count', 'COUNT(*)'),
                     ('min', 'MIN(CAST(l_extendedprice * 100 AS BIGINT))'),
                     ('max', 'MAX(CAST(l_extendedprice * 100 AS BIGINT))')]:
        add(f'tpch_{op}', tpch,
            [{'op': op, 'id': 13, 'bias': 2147483648}] if op != 'count' else [{'op': 'count'}],
            sql=f'SELECT l_returnflag, {expr} FROM lineitem GROUP BY 1',
            db=tpchdb)
    add('tpch_sum_having', tpch, [{'op': 'sum', 'id': 13, 'bias': 2147483648,
                                  'having': {'min': 1000000000000}}],
        sql='SELECT l_returnflag, SUM(CAST(l_extendedprice * 100 AS BIGINT)) FROM lineitem GROUP BY 1 HAVING SUM(CAST(l_extendedprice * 100 AS BIGINT)) >= 1000000000000',
        db=tpchdb)
    add('tpch_count_having', tpch, [{'op': 'count', 'having': {'min': 1000000}}],
        sql='SELECT l_returnflag, COUNT(*) FROM lineitem GROUP BY 1 HAVING COUNT(*) >= 1000000',
        db=tpchdb)
    # Numeric range filters on real/generated 12M cases and IG/2.
    add('nyc_sum_fare_pos', nyc, [{'op': 'sum', 'id': 13, 'bias': 2147483648}],
        sql='SELECT passenger_count, SUM(fare_cents) FROM nyc WHERE passenger_count IS NOT NULL AND fare_cents >= 0 GROUP BY 1',
        db=nycdb)
    specs[-1]['spec']['numeric_filters'] = [{'id': 13, 'min': 0}]
    add('nyc_count_fare_range', nyc, [{'op': 'count'}],
        sql='SELECT passenger_count, COUNT(*) FROM nyc WHERE passenger_count IS NOT NULL AND fare_cents BETWEEN 0 AND 100000 GROUP BY 1',
        db=nycdb)
    specs[-1]['spec']['numeric_filters'] = [{'id': 13, 'min': 0, 'max': 100000}]
    add('nyc_min_fare_pos', nyc, [{'op': 'min', 'id': 13, 'bias': 2147483648}],
        sql='SELECT passenger_count, MIN(fare_cents) FROM nyc WHERE passenger_count IS NOT NULL AND fare_cents >= 0 GROUP BY 1',
        db=nycdb)
    specs[-1]['spec']['numeric_filters'] = [{'id': 13, 'min': 0}]
    add('tpch_sum_price_high', tpch, [{'op': 'sum', 'id': 13, 'bias': 2147483648}],
        sql='SELECT l_returnflag, SUM(CAST(l_extendedprice * 100 AS BIGINT)) FROM lineitem WHERE CAST(l_extendedprice * 100 AS BIGINT) >= 100000 GROUP BY 1',
        db=tpchdb)
    specs[-1]['spec']['numeric_filters'] = [{'id': 13, 'min': 100000}]
    add('tpch_count_price_range', tpch, [{'op': 'count'}],
        sql='SELECT l_returnflag, COUNT(*) FROM lineitem WHERE CAST(l_extendedprice * 100 AS BIGINT) BETWEEN 0 AND 1000000 GROUP BY 1',
        db=tpchdb)
    specs[-1]['spec']['numeric_filters'] = [{'id': 13, 'min': 0, 'max': 1000000}]
    add('ig2_sum_pop_high', ig2, [{'op': 'sum', 'id': 13, 'bias': 2147483648}],
        sql=f'SELECT \"State\", SUM(CAST(\"POPESTIMATE2014\" AS BIGINT)) FROM {t} WHERE \"POPESTIMATE2014\" >= 1000000 GROUP BY 1',
        db=igdb)
    specs[-1]['spec']['numeric_filters'] = [{'id': 13, 'min': 1000000}]
    cmcdb = 'results/citymax14_prep_v3/Observed_duckdb/full31.duckdb'
    add('cmc_count', cmc, [{'op': 'count'}],
        sql='SELECT "City", COUNT(*) FROM "CityMaxCapita_1" WHERE "City" IS NOT NULL GROUP BY 1', db=cmcdb)
    add('cmc_count_top', cmc, [{'op': 'count'}], where_in=['Manhattan', 'Chicago', 'Washington'],
        sql='SELECT "City", COUNT(*) FROM "CityMaxCapita_1" WHERE "City" IN (\'Manhattan\',\'Chicago\',\'Washington\') GROUP BY 1', db=cmcdb)
    return specs


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--timing', action='store_true')
    ap.add_argument('--reps', type=int, default=15)
    args = ap.parse_args()
    import duckdb
    results = []
    for item in build_specs():
        name, spec, sql, db = item['name'], item['spec'], item['sql'], item['db']
        spec_path = OUT / f'{name}.spec.json'
        spec_path.write_text(json.dumps(spec) + '\n')
        expected = canonical(sql_rows(ROOT / db, sql))
        out_path = OUT / f'{name}.result.json'
        proc = subprocess.run([str(EVAL), str(spec_path), str(out_path)], cwd=str(ROOT),
                              capture_output=True, text=True, timeout=120)
        if proc.returncode != 0:
            results.append({'name': name, 'status': 'failed', 'error': proc.stderr[-500:]})
            print(name, 'FAILED', proc.stderr[-300:]); continue
        got = json.loads(out_path.read_text())
        rows = canonical(got['rows'])
        exact = rows == expected
        rec = {'name': name, 'status': 'passed' if exact else 'mismatch',
               'rows': len(rows), 'expected_rows': len(expected), 'exact': exact}
        if args.timing and exact:
            times = []
            for _ in range(args.reps):
                t0 = time.perf_counter()
                subprocess.run([str(EVAL), str(spec_path), str(out_path)], cwd=str(ROOT),
                               capture_output=True, text=True, timeout=120, check=True)
                times.append(json.loads((Path(str(out_path) + '.metrics.json')).read_text())['stored_ms'])
            rec['generic_median_ms'] = statistics.median(times)
            t0 = time.perf_counter()
            con = duckdb.connect(str(ROOT / db), read_only=True, config={'threads': '1'})
            for _ in range(args.reps):
                con.execute(sql).fetchall()
            rec['duckdb_warm_median_ms'] = (time.perf_counter() - t0) * 1000.0 / args.reps
            con.close()
            rec['speedup'] = rec['duckdb_warm_median_ms'] / rec['generic_median_ms']
        results.append(rec)
        print(name, rec)
    (OUT / 'analysis.json').write_text(json.dumps({'results': results}, indent=2) + '\n')


if __name__ == '__main__':
    main()
