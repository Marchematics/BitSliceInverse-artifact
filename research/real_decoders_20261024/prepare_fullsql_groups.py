#!/usr/bin/env python3
"""Source-qualified four-query typed grouping preparation and actual DuckDB bridge.

No WHERE, predicate-compiler benchmark, new leaf kernel or general SQL parser.
Only execute from a separately frozen/CPU-granted gate. Dialect admission on
original schemas and authored rows must precede all observed-source preparation.
"""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parent))
import artifact_paths as _artifact
import argparse
import datetime as dt
import hashlib
import json
import math
import os
from pathlib import Path
import re
import resource
import struct
import sys
import time

ROOT=Path(__file__).resolve().parent
WS=ROOT.parents[1]
COHORT_SHA='c291344d257095e52cb2995c2374c12c0db6859567aeb85f9a9d794f04b36984'
DUCK_NATIVE=WS/'bitslice_external_20261021/deps/site/_duckdb.abi3.so'
DUCK_SHA='e7cda112749d860a36cb1524ff465cefecdb0acc5c4e2ff4510ecc1b6f6c061a'
SOURCE_COMMIT='cf9909083bf9ed185cd7f5121a2881e1fea0e7a9'
DATA_COMMIT='e5fab9e21e0037a6a3f54633c73a3524d08fe8f3'
PREFIX='source/PublicBI_fullsql/benchmark__'
TABLES={
 'Food':{'table':'Food_1','columns':6,'rows':65536,'bytes':2043925,
    'csv_sha256':'c04892ea46c7ed1fbf09f1700d9152023deb110bfb47090c5cabfe370ee8e513',
    'csv_git_sha1':'40a6eeb5d643197d68798b96f03894604400a7a0',
    'ddl_sha256':'d32878f9ac9f2a5f253d026fcd454838f607be39aaa971cbbff103f038fb3267'},
 'IGlocations1':{'table':'IGlocations1_1','columns':18,'rows':65536,'bytes':5453836,
    'csv_sha256':'186979a35563c6b6a7e88b21b5385e4ed70d131b852770d0c2423c2a76c2a447',
    'csv_git_sha1':'ee275641f7c34fcb258559be100f83cf088b927c',
    'ddl_sha256':'bd070cf6fed9e82b84ea20636b13ab014cb69970892dc452c8039f96611da0bc'},
 'IUBLibrary':{'table':'IUBLibrary_1','columns':27,'rows':1795,'bytes':453969,
    'csv_sha256':'333660e1abbf44ddc6f8216779b425186f6a37028341821630658d68670e856e',
    'csv_git_sha1':'405d3ce9144d61aff8781f848040a418e3b0bf01',
    'ddl_sha256':'28ad4b0608d4e4042f6b45654ef775fb0ece9341c4b3c4cb59bdd7ed05f97106'}}
PROFILES={
 'food1':{'source':'Food','query':1,'projected':[3],'ordered':True,
    'sql_sha256':'f5498a40629db32bc39027c60f712cbfbd3c09074fad0b958081747b5d38d85a',
    'columns':[{'name':'device','type':'VARCHAR'}]},
 'ig1':{'source':'IGlocations1','query':1,'projected':[17],'ordered':True,
    'sql_sha256':'d1b467eb9276fbe0cf53e5d115a9fd2f40d720ff8b085e3a407fbb38f84f80e8',
    'columns':[{'name':'State','type':'VARCHAR'}]},
 'iub1':{'source':'IUBLibrary','query':1,'projected':[6,17,18,26],'ordered':False,
    'sql_sha256':'adb4208a44b48ed8d1fac47dba376d577cb5457b36571e310c67ec460c85af49',
    'columns':[{'name':'Calculation_649925789325832192','type':'VARCHAR'},
        {'name':'PubYear','type':'BIGINT'},{'name':'Sh','type':'VARCHAR'},
        {'name':'usr:Number of Records:ok','type':'HUGEINT'},
        {'name':'yr:DateLastCharged:ok','type':'BIGINT'}]},
 'iub4':{'source':'IUBLibrary','query':4,'projected':[9,18,25],'ordered':False,
    'sql_sha256':'5ac8d9210bc1d611c735c44d7ee75c025eac6deb7a5241a9a1a1f39e362782d7',
    'columns':[{'name':'Inactive (group)','type':'VARCHAR'},{'name':'Inactive','type':'VARCHAR'},
               {'name':'Sh','type':'VARCHAR'}]}}
PROFILE_ORDER=['food1','ig1','iub1','iub4']
BASE_CASES=['Observed','MixedTail','Endpoints','Singleton','Empty']
FIXTURE_ROWS={'MixedTail':1031,'Endpoints':65,'Singleton':1,'Empty':0,'AllNullInactive':64}
DATE_CYCLE=['0001-01-01','0001-12-31','1600-02-29','1700-02-28','1700-03-01',
 '1800-02-28','1800-03-01','1899-12-31','1900-01-01','1900-02-28','1900-03-01',
 '1900-12-31','1901-01-01','1969-12-31','1970-01-01','2000-02-28','2000-02-29',
 '2000-03-01','2100-02-28','2100-03-01','2400-02-29','9999-12-31']
TEXT_ONE=['','A','a','é','😀','\n','"','\\','\u2028','\u2029']
TEXT_MANY=['','A','a','é','e\u0301','😀','\n','"\\','\u2028','\u2029','null']
PUBYEARS=[-32768,-1,0,1,32767]
IUB1_BINDING_ROWS=[(-32768,'1900-01-01'),(-32768,'1900-12-31'),(-32768,'1901-01-01'),
 (0,'1900-01-01'),(32767,'2000-02-29'),(1900,'2000-01-01'),(2000,'1900-01-01'),
 (-1,'0001-01-01'),(-1,'9999-12-31')]
DOUBLE_HEX=['0x0.0p+0','-0x0.0p+0','0x0.0000000000001p-1022','-0x0.0000000000001p-1022',
 '0x1.0000000000000p-1022','0x1.fffffffffffffp+1023','-0x1.fffffffffffffp+1023',
 '0x1.0000000000000p+0','-0x1.0000000000000p+0','0x1.999999999999ap-4']
INTEGER=re.compile(r'[+-]?[0-9]+',re.ASCII)
DOUBLE=re.compile(r'[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?',re.ASCII)
DATE=re.compile(r'[0-9]{4}-[0-9]{2}-[0-9]{2}',re.ASCII)


def require(ok,message):
    if not ok:raise RuntimeError(message)


def sha(path):
    h=hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda:stream.read(1<<20),b''):h.update(chunk)
    return h.hexdigest()


def digest(data):
    return {'bytes':len(data),'sha256':hashlib.sha256(data).hexdigest(),
            'git_sha1':hashlib.sha1(b'blob '+str(len(data)).encode()+b'\0'+data).hexdigest()}


def read_json(path):return json.loads(path.read_bytes().decode('utf-8',errors='strict'))


def write_json(path,value):
    with path.open('x',encoding='utf-8',newline='\n') as stream:
        json.dump(value,stream,indent=2,ensure_ascii=False,allow_nan=False);stream.write('\n')


def write_bytes(path,data):
    with path.open('xb') as stream:stream.write(data)
    require(path.read_bytes()==data,'prepared write/readback')


def bitmap(flags):
    out=bytearray(8*((len(flags)+63)//64))
    for i,flag in enumerate(flags):
        if flag:out[i//8]|=1<<(i%8)
    return bytes(out)


def paths(profile):
    require(profile in PROFILES,'fixed profile')
    p=PROFILES[profile];t=TABLES[p['source']]
    return (ROOT/(PREFIX+p['source']+'__queries__'+str(p['query'])+'.sql'),
            ROOT/(PREFIX+p['source']+'__tables__'+t['table']+'.table.sql'),
            ROOT/'input_round3'/ (t['table']+'.csv'))


def case_order(profile):return BASE_CASES+(['AllNullInactive'] if profile=='iub4' else [])


def checked_date(token):
    require(type(token) is str and DATE.fullmatch(token) is not None,'finite canonical CE DATE YYYY-MM-DD')
    value=dt.date(int(token[:4]),int(token[5:7]),int(token[8:10]))
    require(value.isoformat()==token,'canonical DATE roundtrip')
    return value


def civil_days(token):
    value=checked_date(token);y,m,d=value.year,value.month,value.day
    before=[0,31,59,90,120,151,181,212,243,273,304,334];prior=y-1
    leap=y%4==0 and (y%100!=0 or y%400==0)
    ordinal=365*prior+prior//4-prior//100+prior//400+before[m-1]+d+(1 if leap and m>2 else 0)
    days=ordinal-719163
    require(days==(value-dt.date(1970,1,1)).days,'independent date32 encoding')
    return days


def base_schema(profile):
    sql,ddl,_=paths(profile);p=PROFILES[profile];t=TABLES[p['source']]
    require(sha(ROOT/'COHORT_ROUND3.json')==COHORT_SHA and sha(sql)==p['sql_sha256'] and
            sha(ddl)==t['ddl_sha256'],'pinned schema and literal SQL identities before source parsing')
    cohort=read_json(ROOT/'COHORT_ROUND3.json')
    require(cohort['data_commit']==DATA_COMMIT,'pinned data repository commit')
    schema=next(table for table in cohort['tables'] if table['name']==p['source'])['columns']
    require(len(schema)==t['columns'] and [c['index'] for c in schema]==list(range(t['columns'])),
            'every original schema column in source order')
    pattern=re.compile(r'  "([^"]+)" (integer|smallint|date|double|varchar\([0-9]+\))( NOT NULL)?[,]?')
    lines=ddl.read_bytes().decode('utf-8',errors='strict').split('\n')
    matches=[pattern.fullmatch(line) for line in lines[1:-2]]
    require(lines[0]=='CREATE TABLE "'+t['table']+'"(' and len(matches)==len(schema) and all(matches),
            'literal original full DDL field grammar')
    for col,m in zip(schema,matches):
        require((col['name'],col['type'],col['nullable'])==(m.group(1),m.group(2),m.group(3) is None),
                'all original DDL/cohort names/types/nullability/order')
    return schema


def parse_value(token,col):
    if token=='null':
        require(col['nullable'],'original source NOT NULL');return None
    typ=col['type']
    if typ in ('integer','smallint'):
        require(INTEGER.fullmatch(token) is not None,'signed integer grammar')
        value=int(token);bits=32 if typ=='integer' else 16
        require(-(1<<(bits-1))<=value<(1<<(bits-1)),'declared signed integer range');return value
    if typ=='date':checked_date(token);return token
    if typ=='double':
        require(DOUBLE.fullmatch(token) is not None,'finite decimal DOUBLE lexical grammar')
        value=float(token);require(math.isfinite(value),'finite source DOUBLE');return value
    bound=re.fullmatch(r'varchar\(([0-9]+)\)',typ)
    require(bound is not None and len(token)<=int(bound.group(1)),'declared VARCHAR character bound')
    return token


def typed_rows(data,schema):
    records=data.split(b'\n')
    if records and records[-1]==b'':records.pop()
    result=[]
    for rid,record in enumerate(records):
        fields=record.split(b'|')
        require(len(fields)==len(schema),f'original field count row{rid}')
        result.append([parse_value(token.decode('utf-8',errors='strict'),col) for token,col in zip(fields,schema)])
    return result


def validate_rows(rows,schema):
    for row in rows:
        require(type(row) is list and len(row)==len(schema),'full typed source row width')
        for value,col in zip(row,schema):
            typ=col['type']
            if value is None:require(col['nullable'],'unchanged original NOT NULL');continue
            if typ in ('integer','smallint'):
                bits=32 if typ=='integer' else 16
                require(type(value) is int and -(1<<(bits-1))<=value<(1<<(bits-1)),'full typed signed range')
            elif typ=='date':checked_date(value)
            elif typ=='double':require(type(value) is float and math.isfinite(value),'full typed finite DOUBLE')
            else:
                bound=re.fullmatch(r'varchar\(([0-9]+)\)',typ)
                require(type(value) is str and bound is not None and len(value)<=int(bound.group(1)),'full typed VARCHAR bound')
                value.encode('utf-8',errors='strict')


def engine_rows(rows,schema):
    return [[checked_date(value) if value is not None and col['type']=='date' else value
             for value,col in zip(row,schema)] for row in rows]


def typed_equal(a,b):
    return type(a) is type(b) and (struct.pack('<d',a)==struct.pack('<d',b) if type(a) is float else a==b)


def text_cycle(width):
    # Fixed type-based choice, not string truncation or observed-data selection.
    return TEXT_ONE if width==1 else [value for value in TEXT_MANY if len(value)<=width]


def fixture_rows(profile,case):
    require(case in FIXTURE_ROWS and (case!='AllNullInactive' or profile=='iub4'),'fixed authored case')
    schema=base_schema(profile);rows=[]
    for i in range(FIXTURE_ROWS[case]):
        j=i%37 if case=='MixedTail' else i
        row=[]
        for col in schema:
            k=col['index'];typ=col['type']
            if col['nullable'] and j%3==0:value=None
            elif typ in ('integer','smallint'):value=0
            elif typ=='date':value=DATE_CYCLE[(j+k)%len(DATE_CYCLE)]
            elif typ=='double':value=float.fromhex(DOUBLE_HEX[j%len(DOUBLE_HEX)])
            else:
                pool=text_cycle(int(re.fullmatch(r'varchar\(([0-9]+)\)',typ).group(1)))
                value=pool[(j+k)%len(pool)]
            row.append(value)
        if profile=='iub1':
            row[6]=DATE_CYCLE[j%len(DATE_CYCLE)];row[17]=PUBYEARS[j%len(PUBYEARS)]
            row[18]=text_cycle(2)[j%len(text_cycle(2))];row[26]=TEXT_ONE[j%len(TEXT_ONE)]
        if profile=='iub1' and j%37<len(IUB1_BINDING_ROWS):
            pub,date=IUB1_BINDING_ROWS[j%37]
            row[26]='A';row[18]='Z';row[17]=pub;row[6]=date
        if profile=='iub4' and case!='AllNullInactive' and j%37<4:
            row[25]='G';row[18]='S';row[9]=[None,'','x','é'][j%37]
        if case=='AllNullInactive':row[9]=None
        rows.append(row)
    validate_rows(rows,schema);return rows


def dialect_rows(profile):
    rows=fixture_rows(profile,'Endpoints')[:37]
    if profile=='iub1':
        # Exact original duplicate PubYear group/alias resolution is tested with
        # deliberately different source PubYear and extracted calendar year.
        specs=IUB1_BINDING_ROWS
        rows=rows[:len(specs)]
        for row,(pub,date) in zip(rows,specs):row[26]='A';row[18]='Z';row[17]=pub;row[6]=date
    validate_rows(rows,base_schema(profile));return rows


def scalar_order(value):
    if value is None:return (0,0)
    return (1,value.encode('utf-8',errors='strict') if type(value) is str else value)


def key_order(key):return tuple(scalar_order(value) for value in key)


def oracle(profile,rows):
    # Original values and standard-library calendar YEAR only: no encoded code,
    # source-width cache, native reader, engine output or saved query result.
    groups={}
    for row in rows:
        if profile=='food1':key=(row[3],)
        elif profile=='ig1':key=(row[17],)
        elif profile=='iub1':key=(row[26],row[17],row[18],checked_date(row[6]).year)
        else:key=(row[25],row[9],row[18])
        groups[key]=groups.get(key,0)+1
    output=[]
    for key in sorted(groups,key=key_order):
        if profile=='iub1':output.append([key[0],str(key[1]),key[2],str(groups[key]),str(key[3])])
        else:output.append(list(key))
    return {'columns':PROFILES[profile]['columns'],'rows':output},bitmap([True]*len(rows))


def output_keys(profile,result):
    require(set(result)=={'columns','rows'} and result['columns']==PROFILES[profile]['columns'] and
            type(result['rows']) is list,'exact original output headers/types')
    keys=[]
    for row in result['rows']:
        require(type(row) is list and len(row)==len(result['columns']),'complete typed tuple width')
        values=[]
        for i,(value,col) in enumerate(zip(row,result['columns'])):
            if value is None:
                require(profile=='iub4' and i==1,'only original nullable Inactive output');values.append(None)
            elif col['type']=='VARCHAR':
                require(type(value) is str,'VARCHAR output type');value.encode('utf-8',errors='strict');values.append(value)
            else:
                require(type(value) is str and re.fullmatch(r'-?(?:0|[1-9][0-9]*)',value) is not None and
                        str(int(value))==value,'canonical exact integer export')
                number=int(value)
                if col['type']=='HUGEINT':require(0<number<(1<<127),'nonempty SUM1 positive HUGEINT')
                else:require(-(1<<63)<=number<(1<<63),'BIGINT output range')
                values.append(number)
        key=(values[0],values[1],values[2],values[4]) if profile=='iub1' else tuple(values)
        keys.append(key)
    require(len(set(keys))==len(keys),'unique original full tuple groups')
    return keys


def canonical_result(profile,result):
    keys=output_keys(profile,result)
    if PROFILES[profile]['ordered']:
        require(keys==sorted(keys,key=key_order),'actual original ASC binary output order')
        return result
    ordered=sorted(zip(keys,result['rows']),key=lambda pair:key_order(pair[0]))
    return {'columns':result['columns'],'rows':[row for _,row in ordered]}


def read_dictionary(path,binding):
    data=path.read_bytes();require(len(data)>=52 and data[:8]==b'BSSTR01\n' and
        data[8:48].decode('ascii')==binding,'dictionary magic/source binding')
    k=struct.unpack_from('<I',data,48)[0];pos=52;keys=[]
    for _ in range(k):
        require(pos+4<=len(data),'dictionary length header')
        length=struct.unpack_from('<I',data,pos)[0];pos+=4
        require(pos+length<=len(data),'dictionary exact key byte length')
        keys.append(data[pos:pos+length].decode('utf-8',errors='strict'));pos+=length
    require(pos==len(data) and len(set(keys))==k,'unique complete dictionary')
    return keys


def restore(folder,profile,rows,binding,schema):
    require((folder/'profile.txt').read_bytes()==(profile+'\n').encode('ascii') and
            (folder/'binding.txt').read_bytes()==(binding+'\n').encode('ascii'),'profile/source binding')
    columns=PROFILES[profile]['projected'];details=[]
    expected_metadata=[]
    for col in columns:
        typ=schema[col]['type'];width=16 if typ=='smallint' else 32
        expected_metadata.append(f'{col} {width} {int(schema[col]["nullable"])} 5 {len(rows)}\n')
        raw=(folder/f'col{col}.u32').read_bytes();packed=(folder/f'col{col}.biased.u{width}').read_bytes()
        require(len(raw)==4*len(rows) and len(packed)==width//8*len(rows),'normalized/true source-width stream lengths')
        valid=(folder/f'col{col}.valid.bitmap').read_bytes()
        require(valid==bitmap([row[col] is not None for row in rows]),'complete original validity and zero tails')
        keys=read_dictionary(folder/f'col{col}.dict',binding) if typ.startswith('varchar(') else None
        for i,row in enumerate(rows):
            u=struct.unpack_from('<I',raw,4*i)[0]
            pu=struct.unpack_from('<H' if width==16 else '<I',packed,width//8*i)[0]
            require(u==pu,'normalized versus source-width packed value')
            if row[col] is None:require(u==0,'canonical raw NULL placeholder');value=None
            elif keys is not None:
                require(0<=u-(1<<31)<len(keys),'dictionary code domain');value=keys[u-(1<<31)]
            elif typ=='date':value=(dt.date(1970,1,1)+dt.timedelta(days=u-(1<<31))).isoformat()
            else:value=u-(1<<(width-1))
            require(typed_equal(value,row[col]),'every original typed projected value independently restored')
        details.append({'column':col,'source_type':typ,'width':width,'nullable':schema[col]['nullable'],
                        'logical_values':len(rows),'dictionary_count':len(keys) if keys is not None else None})
    require((folder/'columns.tsv').read_bytes()==''.join(expected_metadata).encode('ascii'),'exact fixed component metadata')
    require((folder/'empty.txt').exists()==(not rows),'empty marker corresponds original empty relation')
    if not rows:require((folder/'empty.txt').read_bytes()==b'1\n','exact empty marker')
    return {'status':'passed','profile':profile,'rows':len(rows),'projected_values':len(columns)*len(rows),
        'columns':details,'all_original_values_and_validity_restored':True,
        'dictionary_strings_restored_against_original_source':True,'true_packed_widths_checked':True,
        'canonical_raw_null_placeholders_checked':True,'empty_source_checked':True,
        'no_precomputed_year_or_group_view':True}


def duckdb_identity():
    require(sha(DUCK_NATIVE)==DUCK_SHA,'pinned external DuckDB binary identity')
    sys.path.insert(0,str(DUCK_NATIVE.parent))
    import duckdb
    import _duckdb
    require(duckdb.__version__=='2.0.0' and Path(_duckdb.__file__).resolve()==DUCK_NATIVE.resolve(),
            'loaded pinned DuckDB version/native file')
    return duckdb,{'version':duckdb.__version__,'python_module':duckdb.__file__,
        'python_module_sha256':sha(Path(duckdb.__file__)),'native_module':_duckdb.__file__,
        'native_module_sha256':sha(Path(_duckdb.__file__)),'cpu_affinity':sorted(os.sched_getaffinity(0)),
        'environment_threads':{key:os.environ.get(key) for key in
            ['OMP_NUM_THREADS','OPENBLAS_NUM_THREADS','MKL_NUM_THREADS','RAYON_NUM_THREADS']}}


def session_settings(con):
    con.execute('SET threads=1').fetchall()
    con.execute('SET preserve_insertion_order=true').fetchall()
    settings={key:con.execute(f"SELECT current_setting('{key}')").fetchall()[0][0]
              for key in ['threads','preserve_insertion_order','default_collation','default_null_order','TimeZone']}
    settings['binary_behavior_probes']=list(con.execute("SELECT 'A'='a','é'='e','é'='é'").fetchall()[0])
    settings['default_ascending_null_probe']=[row[0] for row in con.execute(
        'SELECT v FROM (VALUES (NULL),(1),(0)) AS t(v) ORDER BY v ASC').fetchall()]
    require(settings['threads']==1 and settings['preserve_insertion_order'] is True and
        settings['default_collation'] in ('','binary') and settings['binary_behavior_probes']==[False,False,False] and
        settings['default_ascending_null_probe'] in ([0,1,None],[None,0,1]),
        'actual single-thread binary session and recorded engine NULL ordering')
    return settings


def engine_result(profile,cursor):
    columns=[{'name':entry[0],'type':str(entry[1])} for entry in cursor.description]
    actual=cursor.fetchall()
    require(columns==PROFILES[profile]['columns'],'actual exact original aliases and promoted result types')
    rows=[]
    for entry in actual:
        require(len(entry)==len(columns),'actual complete tuple cardinality')
        row=[]
        for value,col in zip(entry,columns):
            if value is None:row.append(None)
            elif col['type']=='VARCHAR':require(type(value) is str,'actual engine text');row.append(value)
            else:require(type(value) is int,'actual engine exact integer');row.append(str(value))
        rows.append(row)
    original={'columns':columns,'rows':rows}
    return canonical_result(profile,original),original


def insert_rows(con,profile,rows,schema):
    table=TABLES[PROFILES[profile]['source']]['table']
    con.execute(paths(profile)[1].read_bytes().decode('utf-8',errors='strict')).fetchall()
    typed=engine_rows(rows,schema)
    if typed:
        con.execute('BEGIN TRANSACTION').fetchall()
        con.executemany('INSERT INTO "'+table+'" VALUES ('+','.join(['?']*len(schema))+')',typed).fetchall()
        con.execute('COMMIT').fetchall()
    return typed


def engine_schema(con,profile,schema):
    table=TABLES[PROFILES[profile]['source']]['table']
    columns=con.execute("PRAGMA table_info('"+table+"')").fetchall()
    require(len(columns)==len(schema),'actual every source-schema column')
    for i,(actual,col) in enumerate(zip(columns,schema)):
        expected='VARCHAR' if col['type'].startswith('varchar(') else col['type'].upper()
        require(actual[0]==i and actual[1]==col['name'] and actual[2]==expected and bool(actual[3]) is (not col['nullable']),
                'all original engine names/types/nullability/order')
    return columns


def dialect(output):
    # No observed CSV/source-row path is opened in this mode. Both relation
    # shapes execute the exact original SQL against the full original schema.
    began=time.perf_counter();receipts=[];identity=None;status='failed';error=None
    try:
        duckdb,identity=duckdb_identity()
        for profile in PROFILE_ORDER:
            schema=base_schema(profile);sql=paths(profile)[0].read_bytes().decode('utf-8',errors='strict')
            for case,rows in [('Empty',[]),('AuthoredNonempty',dialect_rows(profile))]:
                con=None
                try:
                    con=duckdb.connect(config={'threads':'1','preserve_insertion_order':'true'})
                    settings=session_settings(con);insert_rows(con,profile,rows,schema)
                    columns=engine_schema(con,profile,schema)
                    expected,_=oracle(profile,rows)
                    result,original=engine_result(profile,con.execute(sql))
                    require(result==expected,'exact original authored dialect full output versus independent oracle')
                    receipts.append({'profile':profile,'case':case,'rows':len(rows),
                        'original_sql_sha256':PROFILES[profile]['sql_sha256'],
                        'original_ddl_sha256':TABLES[PROFILES[profile]['source']]['ddl_sha256'],
                        'actual_schema_columns':columns,'settings':settings,'actual_cursor_description':original['columns'],
                        'original_engine_returned_sequence':original['rows'],'canonical_complete_result':result,
                        'independent_oracle':expected,'complete_typed_result_equal':True,
                        'original_SQL_rewritten':False,'original_NOT_NULL_unchanged':True})
                finally:
                    if con is not None:con.close()
        require(len(receipts)==8,'all four exact SQL dialects on both empty/nonempty relations')
        status='passed'
    except BaseException as exc:error=f'{type(exc).__name__}: {exc}'
    receipt={'status':status,'error':error,'stage':'authored exact-original-SQL dialect before observed preparation',
        'profiles':PROFILE_ORDER,'executions':receipts,'completed_original_queries':len(receipts),
        'identity':identity,'preparer_source_sha256':sha(Path(__file__)),
        'observed_values_used':False,'query_coverage_increment':0,'no_timing_claim':True,
        'iub1_duplicate_alias_binding_requires_independent_tuples':True,
        'original_sql_commit':SOURCE_COMMIT,'wall_ms':(time.perf_counter()-began)*1000}
    write_json(output,receipt)
    if status!='passed':raise RuntimeError(error)


def check_dialect(path):
    receipt=read_json(path)
    require(receipt['status']=='passed' and receipt['profiles']==PROFILE_ORDER and
        receipt['preparer_source_sha256']==sha(Path(__file__)) and receipt['observed_values_used'] is False and
        receipt['query_coverage_increment']==0 and receipt['completed_original_queries']==8 and
        receipt['identity']['version']=='2.0.0' and receipt['identity']['native_module_sha256']==DUCK_SHA,
        'prior exact authored dialect receipt required before any observed CSV read')
    require([(row['profile'],row['case']) for row in receipt['executions']]==[
        (profile,case) for profile in PROFILE_ORDER for case in ['Empty','AuthoredNonempty']],
        'complete fixed original dialect denominator')
    for row in receipt['executions']:
        p=row['profile'];t=TABLES[PROFILES[p]['source']]
        require(row['original_sql_sha256']==PROFILES[p]['sql_sha256'] and row['original_ddl_sha256']==t['ddl_sha256'] and
                row['actual_cursor_description']==PROFILES[p]['columns'] and row['complete_typed_result_equal'] is True and
                row['original_SQL_rewritten'] is False and row['original_NOT_NULL_unchanged'] is True,
                'literal unchanged original dialect/schema/result identity')
        expected,_=oracle(p,[] if row['case']=='Empty' else dialect_rows(p))
        require(row['canonical_complete_result']==row['independent_oracle']==expected and
                canonical_result(p,{'columns':row['actual_cursor_description'],
                                     'rows':row['original_engine_returned_sequence']})==expected,
                'complete authored dialect evidence rechecked before data admission')
    return receipt


def prepare(output,dialect_path):
    # This assertion is deliberately before all CSV reads, including hashing.
    dialect_receipt=check_dialect(dialect_path)
    schemas={p:base_schema(p) for p in PROFILE_ORDER}
    acquisition=read_json(ROOT/'COHORT_ACQUISITION_RECEIPT.json')
    require(acquisition['all_sources_exact'] is True,'original acquisition receipt')
    output.mkdir(exist_ok=False);cases=[];source_rows={};source_records=[]
    for profile in PROFILE_ORDER:
        p=PROFILES[profile];t=TABLES[p['source']];sql,ddl,csv=paths(profile);schema=schemas[profile]
        if p['source'] not in source_rows:
            acquired=next(row for row in acquisition['files'] if row['table']==p['source'])
            require(acquired['source_commit']==DATA_COMMIT and acquired['sha256']==t['csv_sha256'] and
                acquired['git_sha1']==t['csv_git_sha1'] and acquired['bytes']==t['bytes'] and
                (ROOT/acquired['path']).resolve()==csv.resolve(),'pinned original CSV provenance')
            data=csv.read_bytes()
            require(digest(data)=={'bytes':t['bytes'],'sha256':t['csv_sha256'],'git_sha1':t['csv_git_sha1']},
                    'all original CSV bytes and lines preserved')
            observed=typed_rows(data,schema)
            require(len(observed)==t['rows'],'all original observed rows; no exclusions')
            validate_rows(observed,schema);source_rows[p['source']]=observed
            source_records.append({'table':p['source'],'path':str(csv),**digest(data),'rows':len(observed),
                'original_CSV_retained_in_place':True,'all_original_fields_validated':True})
        parent=output/profile;parent.mkdir()
        for case in case_order(profile):
            folder=parent/case;folder.mkdir()
            rows=source_rows[p['source']] if case=='Observed' else fixture_rows(profile,case)
            validate_rows(rows,schema);require(len(rows)<=1000000,'native bounded exact source rows')
            raw_rows=''.join(json.dumps(row,ensure_ascii=False,separators=(',',':'),allow_nan=False)+'\n'
                             for row in rows).encode('utf-8')
            # Exact JSON binary64 roundtrip, including signed zero, is checked
            # before any engine import rather than assumed from numeric equality.
            reload=[json.loads(line) for line in raw_rows.decode('utf-8').split('\n')[:-1]] if raw_rows else []
            require(len(reload)==len(rows) and all(all(typed_equal(a,b) for a,b in zip(x,y))
                for x,y in zip(reload,rows)),'all full-source JSON types/values and DOUBLE bits preserved')
            binding=t['csv_git_sha1'] if case=='Observed' else digest(raw_rows)['git_sha1']
            write_bytes(folder/'profile.txt',(profile+'\n').encode('ascii'))
            write_bytes(folder/'binding.txt',(binding+'\n').encode('ascii'))
            write_bytes(folder/'query.sql',sql.read_bytes());write_bytes(folder/'schema.sql',ddl.read_bytes())
            write_bytes(folder/'source_rows.jsonl',raw_rows)
            metadata=[];dictionary_records=[]
            for col in p['projected']:
                typ=schema[col]['type'];width=16 if typ=='smallint' else 32
                metadata.append(f'{col} {width} {int(schema[col]["nullable"])} 5 {len(rows)}\n')
                code=None
                if typ.startswith('varchar('):
                    dictionary=[];code={}
                    for row in rows:
                        value=row[col]
                        if value is not None and value not in code:code[value]=len(dictionary);dictionary.append(value)
                    data=bytearray(b'BSSTR01\n'+binding.encode('ascii')+struct.pack('<I',len(dictionary)))
                    for value in dictionary:
                        key=value.encode('utf-8',errors='strict');require(len(key)<=1024,'native literal UTF8 entry bound')
                        data.extend(struct.pack('<I',len(key))+key)
                    require(len(data)<=64*1024*1024,'per-column source dictionary budget')
                    write_bytes(folder/f'col{col}.dict',bytes(data))
                    dictionary_records.append({'column':col,'cardinality':len(dictionary),'bytes':len(data),
                                               'first_occurrence_original_strings':True})
                normalized=bytearray();packed=bytearray()
                for row in rows:
                    value=row[col]
                    if value is None:biased=0
                    else:
                        original=code[value] if code is not None else civil_days(value) if typ=='date' else value
                        biased=original+(1<<(width-1))
                        require(0<=biased<(1<<width),'exact biased declared source domain')
                    normalized.extend(struct.pack('<I',biased))
                    packed.extend(struct.pack('<H' if width==16 else '<I',biased))
                write_bytes(folder/f'col{col}.u32',bytes(normalized))
                write_bytes(folder/f'col{col}.biased.u{width}',bytes(packed))
                write_bytes(folder/f'col{col}.valid.bitmap',bitmap([row[col] is not None for row in rows]))
            write_bytes(folder/'columns.tsv',''.join(metadata).encode('ascii'))
            if not rows:write_bytes(folder/'empty.txt',b'1\n')
            expected,all_rows=oracle(profile,rows)
            write_json(folder/'oracle.json',expected);write_bytes(folder/'oracle.bitmap',all_rows)
            restoration=restore(folder,profile,rows,binding,schema)
            write_json(folder/'RESTORATION_RECEIPT.json',restoration)
            record={'profile':profile,'case':case,'rows':len(rows),'binding':binding,
                'original_projected_columns':p['projected'],'full_schema_columns':len(schema),
                'projected_source_types':[schema[col]['type'] for col in p['projected']],
                'projected_nullable':[schema[col]['nullable'] for col in p['projected']],
                'original_NOT_NULL_unchanged_all_cases':True,'schema_sha256':sha(folder/'schema.sql'),
                'original_sql_sha256':p['sql_sha256'],'source_rows_sha256':sha(folder/'source_rows.jsonl'),
                'source_csv':str(csv) if case=='Observed' else None,
                'source_csv_sha256':t['csv_sha256'] if case=='Observed' else None,
                'observed_original_query':case=='Observed','dictionaries':dictionary_records,
                'ordered_original_SQL':p['ordered'],'sum_literal_one':profile=='iub1','no_WHERE':True,
                'representation':'original finite DATE32 / SMALLINT16 / literal original UTF8 code32; fixed family5',
                'no_year_cache_or_group_index':True,'full_typed_output_columns':p['columns'],
                'files':{path.name:digest(path.read_bytes()) for path in sorted(folder.iterdir()) if path.is_file()}}
            write_json(folder/'case.json',record);cases.append(record)
    require(len(cases)==21,'fixed four-profile21-case roster')
    write_json(output/'PREPARATION_RECEIPT.json',{'status':'passed','cases':cases,'sources':source_records,
        'dialect_receipt_path':str(dialect_path),'dialect_receipt_sha256':sha(dialect_path),
        'dialect_before_observed_reads':True,'authored_dialect_executions':len(dialect_receipt['executions']),
        'all_original_rows_and_CSV_lines_preserved':True,'all_full_schema_original_values_validated':True,
        'full_projected_original_values_and_validity_restored':True,'original_NOT_NULL_unchanged_all_cases':True,
        'fixed_codecs_independent_of_values_and_query_outputs':True,'new_projection_frozen_before_inspection':True,
        'fresh_unseen_dataset_claim':False,'no_WHERE_no_predicate_compiler_speed_claim':True,'no_timing_claim':True})


def checked_case(folder):
    meta=read_json(folder/'case.json');profile=meta['profile']
    require(profile in PROFILES and meta['case'] in case_order(profile),'fixed prepared profile/case')
    p=PROFILES[profile];t=TABLES[p['source']]
    require(meta['rows']==(t['rows'] if meta['case']=='Observed' else FIXTURE_ROWS[meta['case']]) and
        (folder/'profile.txt').read_bytes()==(profile+'\n').encode('ascii') and
        sha(folder/'query.sql')==p['sql_sha256'] and sha(folder/'schema.sql')==t['ddl_sha256'] and
        meta['schema_sha256']==t['ddl_sha256'] and meta['original_NOT_NULL_unchanged_all_cases'] is True,
        'exact original SQL/DDL/profile/rows with no nullable fixture relaxation')
    return meta


def duck_build(folder,storage):
    began=time.perf_counter();duckdb,identity=duckdb_identity();meta=checked_case(folder)
    profile=meta['profile'];schema=base_schema(profile);table=TABLES[PROFILES[profile]['source']]['table']
    require(sha(folder/'source_rows.jsonl')==meta['source_rows_sha256'],'all original full typed rows identity')
    raw=(folder/'source_rows.jsonl').read_bytes().decode('utf-8',errors='strict')
    require(not raw or raw.endswith('\n'),'JSONL exact LF record boundary')
    rows=[json.loads(line) for line in raw.split('\n')[:-1]] if raw else []
    require(len(rows)==meta['rows'],'full original row count');validate_rows(rows,schema)
    source_read_ms=(time.perf_counter()-began)*1000
    storage.mkdir(exist_ok=False);database=storage/'fulltable.duckdb';con=None
    try:
        con=duckdb.connect(str(database),config={'threads':'1','preserve_insertion_order':'true'})
        settings=session_settings(con);tick=time.perf_counter()
        typed=insert_rows(con,profile,rows,schema);columns=engine_schema(con,profile,schema)
        con.execute('CHECKPOINT').fetchall();con.close();con=None
        build_ms=(time.perf_counter()-tick)*1000
        con=duckdb.connect(str(database),read_only=True,config={'threads':'1','preserve_insertion_order':'true'})
        reopen_settings=session_settings(con)
        fetched=con.execute('SELECT rowid,* FROM "'+table+'" ORDER BY rowid').fetchall()
        require(len(fetched)==len(typed),'persisted original fulltable row count')
        for i,(actual,expected) in enumerate(zip(fetched,typed)):
            require(len(actual)==len(schema)+1 and actual[0]==i and
                all(typed_equal(a,b) for a,b in zip(actual[1:],expected)),
                'every original full-schema typed value/date/DOUBLE bit pattern restored')
        con.close();con=None
    finally:
        if con is not None:con.close()
    write_json(storage/'build_receipt.json',{'status':'passed','method':'duckdb','profile':profile,'case':meta['case'],
        'identity':identity,'settings':settings,'reopen_settings':reopen_settings,'rows':len(rows),
        'full_schema_columns':len(schema),'full_value_cells_verified':len(schema)*len(rows),'schema_columns':columns,
        'date_columns_typed_roundtripped':[c['index'] for c in schema if c['type']=='date'],
        'double_columns_bitwise_roundtripped':[c['index'] for c in schema if c['type']=='double'],
        'DOUBLE_including_signed_zero_not_normalized':True,'source_read_and_identity_ms':source_read_ms,
        'literal_ddl_insert_checkpoint_ms':build_ms,'build_save_validate_ms':(time.perf_counter()-began)*1000,
        'database_bytes':database.stat().st_size,'database_sha256':sha(database),
        'source_rows_sha256':meta['source_rows_sha256'],'original_sql_sha256':meta['original_sql_sha256'],
        'schema_sha256':meta['schema_sha256'],'original_NOT_NULL_unchanged':True,
        'ingestion':'full typed executemany correctness bridge; not independent CSV parsing or competitive ingestion',
        'fulltable_vs_projected_compression_ratio_not_defined':True,
        'peak_process_rss_kib':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,'no_timing_claim':True})


def duck_query(folder,storage,resultpath,bitmappath,metricspath):
    began=time.perf_counter();duckdb,identity=duckdb_identity();meta=checked_case(folder)
    profile=meta['profile'];receipt=read_json(storage/'build_receipt.json');database=storage/'fulltable.duckdb'
    require(receipt['status']=='passed' and receipt['profile']==profile and receipt['case']==meta['case'] and
        receipt['source_rows_sha256']==meta['source_rows_sha256'] and sha(database)==receipt['database_sha256'],
        'actual complete persisted source identity')
    con=None;diagnostic=bitmappath!='-';diag_ms=0.0;bitmap_bytes=0
    try:
        con=duckdb.connect(str(database),read_only=True,config={'threads':'1','preserve_insertion_order':'true'})
        settings=session_settings(con);setup_ms=(time.perf_counter()-began)*1000
        tick=time.perf_counter()
        result,original=engine_result(profile,con.execute((folder/'query.sql').read_bytes().decode('utf-8',errors='strict')))
        execute_fetch_export_ms=(time.perf_counter()-tick)*1000
        write_json(resultpath,result)
        if diagnostic:
            tick=time.perf_counter();table=TABLES[PROFILES[profile]['source']]['table']
            actual=con.execute('SELECT rowid FROM "'+table+'" ORDER BY rowid').fetchall()
            require(len(actual)==meta['rows'] and all(row==(i,) for i,row in enumerate(actual)),
                    'diagnostic all original row IDs for no-WHERE query')
            data=bitmap([True]*len(actual));write_bytes(Path(bitmappath),data);bitmap_bytes=len(data)
            diag_ms=(time.perf_counter()-tick)*1000
        con.close();con=None
    finally:
        if con is not None:con.close()
    write_json(metricspath,{'method':'duckdb','profile':profile,'case':meta['case'],'rows':meta['rows'],
        'identity':identity,'settings':settings,'actual_cursor_description':original['columns'],
        'original_engine_returned_sequence':original['rows'],'original_order_required':PROFILES[profile]['ordered'],
        'unordered_complete_tuple_canonicalization_only':not PROFILES[profile]['ordered'],
        'setup_ms':setup_ms,'execute_fetch_export_ms':execute_fetch_export_ms,'result_bytes':resultpath.stat().st_size,
        'primary_original_SQL_executions':1,'original_SQL_rewritten':False,'diagnostic_enabled':diagnostic,
        'diagnostic_allrows_bitmap_not_predicate_compilation':True,'diagnostic_bitmap_bytes':bitmap_bytes,
        'diagnostic_auxiliary_query_count':1 if diagnostic else 0,'diagnostic_ms_separate':diag_ms,
        'query_worker_does_not_read_source_rows_or_oracle':True,'lowered_predicates':0,'no_WHERE':True,
        'sum_literal_one':profile=='iub1','full_worker_ms_before_metrics':(time.perf_counter()-began)*1000,
        'peak_process_rss_kib':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,'no_timing_claim':True})


def selftest(output):
    schemas={p:base_schema(p) for p in PROFILE_ORDER}
    fields={p:[b'0' if c['type'] in ('integer','smallint') else b'0.0' if c['type']=='double' else
        b'1970-01-01' if c['type']=='date' else b'A' for c in schema] for p,schema in schemas.items()}
    cases=[('food_device_not_null','food1',3,b'null'),('food_double_not_null','food1',5,b'null'),
        ('double_nan','food1',5,b'NaN'),('double_infinity','food1',5,b'inf'),
        ('double_overflow','food1',5,b'1e309'),('double_hex_not_source_grammar','food1',5,b'0x1p0'),
        ('double_whitespace','food1',5,b' 1.0'),('food_integer_overflow','food1',1,b'2147483648'),
        ('food_smallint_underflow','food1',0,b'-32769'),('food_device_overlength','food1',3,b'x'*41),
        ('invalid_UTF8','food1',3,bytes([255])),('ig_state_not_null','ig1',17,b'null'),
        ('ig_state_overlength','ig1',17,b'x'*21),('ig_integer_underflow','ig1',1,b'-2147483649'),
        ('date_not_null','iub1',6,b'null'),('sh_not_null','iub1',18,b'null'),
        ('calculation_not_null','iub1',26,b'null'),('PubYear_overflow','iub1',17,b'32768'),
        ('PubYear_underflow','iub1',17,b'-32769'),('date_invalid_century_leap','iub1',6,b'1900-02-29'),
        ('date_zero_year','iub1',6,b'0000-01-01'),('date_year10000','iub1',6,b'10000-01-01'),
        ('date_noncanonical','iub1',6,b'1900-1-01'),('date_infinity','iub1',6,b'infinity'),
        ('date_whitespace','iub1',6,b'1900-01-01 '),('sh_overlength','iub1',18,b'xxx'),
        ('calculation_overlength','iub1',26,b'xx')]
    rejected=[]
    for name,profile,col,value in cases:
        row=list(fields[profile]);row[col]=value
        try:typed_rows(b'|'.join(row)+b'\n',schemas[profile])
        except (RuntimeError,ValueError,UnicodeDecodeError,OverflowError):rejected.append(name)
        else:raise RuntimeError('missing bounded original-source rejection '+name)
    for spelling in DOUBLE_HEX:
        value=float.fromhex(spelling);row=list(fields['food1']);row[5]=repr(value).encode('ascii')
        actual=typed_rows(b'|'.join(row)+b'\n',schemas['food1'])[0][5]
        require(typed_equal(actual,value) and typed_equal(json.loads(json.dumps(value,allow_nan=False)),value),
                'finite original DOUBLE parse/JSON exact bits including signed zero')
    for token in DATE_CYCLE:
        days=civil_days(token)
        require((dt.date(1970,1,1)+dt.timedelta(days=days)).isoformat()==token,'finite DATE domain endpoints')
    expected,_=oracle('iub1',dialect_rows('iub1'))
    require(expected['rows']==[['A','-32768','Z','2','1900'],['A','-32768','Z','1','1901'],
        ['A','-1','Z','1','1'],['A','-1','Z','1','9999'],['A','0','Z','1','1900'],
        ['A','1900','Z','1','2000'],['A','2000','Z','1','1900'],['A','32767','Z','1','2000']],
        'source-only same-year merge, cross-year split, and independent PubYear key')
    distinct,_=oracle('iub4',dialect_rows('iub4'))
    require(all(row in distinct['rows'] for row in [['G',None,'S'],['G','','S'],['G','x','S'],['G','é','S']]),
            'original nullable Inactive distinct from empty with same other columns')
    fixtures=0
    for profile in PROFILE_ORDER:
        for case in case_order(profile):
            if case=='Observed':continue
            rows=fixture_rows(profile,case);validate_rows(rows,schemas[profile]);result,allrows=oracle(profile,rows)
            require(result==canonical_result(profile,result) and allrows==bitmap([True]*len(rows)),
                    'complete typed fixture oracle and no-WHERE row coverage')
            fixtures+=1
    write_json(output,{'group_preparer_helpers':'PASS','source_rejections':rejected,
        'finite_DOUBLE_bitwise_cases':len(DOUBLE_HEX),'date_roundtrip_cases':len(DATE_CYCLE),
        'authored_fixture_profiles_cases':fixtures,'iub1_alias_tuple_rows':len(expected['rows']),
        'same_key_NULL_empty_Inactive_cases':4,'original_NOT_NULL_unchanged':True,
        'observed_values_used':False,'no_general_malformed_assurance':True})


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('mode',choices=['prepare','selftest','dialect','identity','duck-build','duck-query'])
    parser.add_argument('paths',nargs='+');args=parser.parse_args()
    if args.mode=='prepare':
        require(len(args.paths)==2,'prepare OUTPUT DIALECT_RECEIPT');prepare(*map(Path,args.paths))
    elif args.mode=='selftest':require(len(args.paths)==1,'selftest OUTPUT');selftest(Path(args.paths[0]))
    elif args.mode=='dialect':require(len(args.paths)==1,'dialect OUTPUT');dialect(Path(args.paths[0]))
    elif args.mode=='identity':
        require(len(args.paths)==1,'identity OUTPUT');_,identity=duckdb_identity();write_json(Path(args.paths[0]),identity)
    elif args.mode=='duck-build':require(len(args.paths)==2,'duck-build CASE STORAGE');duck_build(*map(Path,args.paths))
    else:
        require(len(args.paths)==5,'duck-query CASE STORAGE RESULT BITMAP_OR_- METRICS')
        folder,storage,result,bm,metrics=args.paths
        duck_query(Path(folder),Path(storage),Path(result),bm,Path(metrics))


if __name__=='__main__':main()
