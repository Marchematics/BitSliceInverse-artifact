#!/usr/bin/env python3
"""Exact Arade/2 source preparation and actual full11 DuckDB qualification.

Source-only authoring is not admission. No source row is dropped or rounded.
Execute only through a separately reviewed/frozen/CPU-granted gate. No timing
panel is implemented here. Primary unordered LIMIT outputs are relationally
verified, not forced to equal one arbitrary engine/native subset.
"""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parent))
import artifact_paths as _artifact
import argparse
import datetime as dt
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import re
import resource
import struct
import sys
import time

ROOT=Path(__file__).resolve().parent
WS=ROOT.parents[1]
SQL=ROOT/'source/PublicBI_fullsql/benchmark__Arade__queries__2.sql'
DDL=ROOT/'source/PublicBI_fullsql/benchmark__Arade__tables__Arade_1.table.sql'
CSV=ROOT/'input_round3/Arade_1.csv'
SQL_SHA='c2b424650d302a6fe4ed26f3ce7cc42400aa1b485e3766c3e31d8deaa9ef8875'
DDL_SHA='1403a4b4046f3f3935ac0b2945baaf53e0b60bd08760a96c2d1ee99ae4599f43'
CSV_SHA='3accee739741d1cd37bc9664e0e92c771e0fca90437943663654981739dba1a3'
CSV_BLOB='37995ef80f78a816f9e437e9362467f8ca969d73'
COHORT_SHA='c291344d257095e52cb2995c2374c12c0db6859567aeb85f9a9d794f04b36984'
DUCK_NATIVE=WS/'bitslice_external_20261021/deps/site/_duckdb.abi3.so'
DUCK_SHA='e7cda112749d860a36cb1524ff465cefecdb0acc5c4e2ff4510ecc1b6f6c061a'
COLUMNS=[{'name':'F4','type':'DECIMAL(8,4)'}]
US_DAY=86400000000
EPOCH_ORDINAL=719163
WORD=1<<32
BIAS=1<<31
MIN_US=-62135596800000000
MAX_US=253402300799999999
TIMESTAMP=re.compile(r'([0-9]{4})-([0-9]{2})-([0-9]{2}) ([0-9]{2}):([0-9]{2}):([0-9]{2})(?:\.([0-9]{1,6}))?',re.ASCII)
DECIMAL_TEXT=re.compile(r'(-?)([0-9]+)(?:\.([0-9]+))?',re.ASCII)
SMALLINT_TEXT=re.compile(r'[+-]?[0-9]+',re.ASCII)
DECIMAL_TYPE=re.compile(r'decimal\(([0-9]+), ([0-9]+)\)',re.ASCII)


def require(ok,message):
    if not ok: raise RuntimeError(message)


def sha(path):
    h=hashlib.sha256()
    with path.open('rb') as stream:
        for part in iter(lambda:stream.read(1<<20),b''):h.update(part)
    return h.hexdigest()


def digest(data):
    return {'bytes':len(data),'sha256':hashlib.sha256(data).hexdigest(),
            'git_sha1':hashlib.sha1(b'blob '+str(len(data)).encode()+b'\0'+data).hexdigest()}


def read_json(path):return json.loads(path.read_bytes().decode('utf-8',errors='strict'))


def write_json(path,value):
    with path.open('x',encoding='utf-8',newline='\n') as stream:
        json.dump(value,stream,ensure_ascii=False,allow_nan=False,indent=2)
        stream.write('\n')


def write_bytes(path,data):
    with path.open('xb') as stream:stream.write(data)
    require(path.read_bytes()==data,'prepared full byte write/readback')


def bitmap(flags):
    out=bytearray(8*((len(flags)+63)//64))
    for i,flag in enumerate(flags):
        if flag:out[i//8]|=1<<(i%8)
    return bytes(out)


def checked_timestamp(token):
    require(type(token) is str,'timestamp original string')
    match=TIMESTAMP.fullmatch(token)
    require(match is not None,'finite naive timestamp lexical grammar')
    parts=[int(match.group(i)) for i in range(1,7)]
    micros=int((match.group(7) or '').ljust(6,'0'))
    # Standard library validates CE year/month/day and full time without local
    # timezone conversion or float timestamp(). Original spelling is preserved.
    return dt.datetime(*parts,micros)


def timestamp_us(token):
    value=checked_timestamp(token)
    y=value.year-1
    before=[0,31,59,90,120,151,181,212,243,273,304,334]
    leap=value.year%4==0 and (value.year%100!=0 or value.year%400==0)
    ordinal=365*y+y//4-y//100+y//400+before[value.month-1]+value.day+(1 if leap and value.month>2 else 0)
    r=((value.hour*60+value.minute)*60+value.second)*1000000+value.microsecond
    integer=(ordinal-EPOCH_ORDINAL)*US_DAY+r
    require(MIN_US<=integer<=MAX_US and -(1<<63)<=integer<(1<<63),'finite checked signed64 timestamp')
    independent=value-dt.datetime(1970,1,1)
    require(integer==independent.days*US_DAY+independent.seconds*1000000+independent.microseconds,
            'independent datetime integer epoch delta')
    return integer


def restored_timestamp(integer):
    require(type(integer) is int and MIN_US<=integer<=MAX_US,'finite timestamp restoration domain')
    day,remainder=divmod(integer,US_DAY)  # Euclidean floor, including -1us.
    seconds,micros=divmod(remainder,1000000)
    return dt.datetime(1970,1,1)+dt.timedelta(days=day,seconds=seconds,microseconds=micros)


def split_timestamp(integer):
    require(type(integer) is int and -(1<<63)<=integer<(1<<63),'full signed64 split domain')
    q,low=divmod(integer,WORD)
    high=q+BIAS
    require(0<=high<WORD and 0<=low<WORD and (high-BIAS)*WORD+low==integer,'exact high/low split and join')
    return high,low


def decimal_coefficient(token,precision,scale):
    require(type(token) is str,'original decimal string')
    match=DECIMAL_TEXT.fullmatch(token)
    require(match is not None,'exact decimal lexical grammar; no plus/exponent/rounding')
    fraction=match.group(3) or ''
    require(len(fraction)<=scale,'declared decimal fractional precision')
    # Remove leading zeros before integer construction, avoiding Python's
    # decimal-digit conversion limit for legal leading-zero spellings.
    digits=(match.group(2)+fraction.ljust(scale,'0')).lstrip('0') or '0'
    require(len(digits)<=precision,'declared total decimal precision')
    coefficient=int(digits)
    require(coefficient<10**precision,'declared decimal magnitude')
    if match.group(1):coefficient=-coefficient
    exact=Decimal((int(coefficient<0),tuple(int(d) for d in str(abs(coefficient))),-scale))
    require(Decimal(token)==exact,'independent exact Decimal coefficient without context rounding')
    return coefficient


def decimal_text(coefficient,scale):
    require(type(coefficient) is int,'decimal coefficient type')
    sign='-' if coefficient<0 else ''
    whole,fraction=divmod(abs(coefficient),10**scale)
    return f'{sign}{whole}.{fraction:0{scale}d}'


def engine_decimal_coefficient(value,precision,scale):
    require(type(value) is Decimal and value.is_finite(),'actual finite Decimal type')
    sign,digits,exponent=value.as_tuple()
    require(type(exponent) is int and exponent>=-scale,'actual engine decimal scale without rounding')
    integer=0
    for digit in digits:integer=integer*10+digit
    integer*=10**(exponent+scale)
    if sign:integer=-integer
    require(abs(integer)<10**precision,'actual Decimal coefficient range')
    return integer


def key_order(value):return (0,0) if value is None else (1,value)


def base_schema():
    require(sha(ROOT/'COHORT_ROUND3.json')==COHORT_SHA and sha(DDL)==DDL_SHA,'pinned cohort/full11 DDL identities')
    schema=next(t for t in read_json(ROOT/'COHORT_ROUND3.json')['tables'] if t['name']=='Arade')['columns']
    require([c['index'] for c in schema]==list(range(11)),'all11 original columns')
    pattern=re.compile(r'  "([^"]+)" (smallint|timestamp|varchar\([0-9]+\)|decimal\([0-9]+, [0-9]+\))( NOT NULL)?[,]?')
    fields=[pattern.fullmatch(line) for line in DDL.read_bytes().decode('utf-8').split('\n')[1:-2]]
    require(len(fields)==11 and all(fields),'exact full11 DDL field grammar')
    for col,match in zip(schema,fields):
        require((col['name'],col['type'],col['nullable'])==(match.group(1),match.group(2),match.group(3) is None),
                'all11 original DDL/cohort names/types/order/nullability')
    require(schema[2]['type']=='timestamp' and schema[3]['type']=='decimal(8, 4)' and
            not schema[2]['nullable'] and not schema[3]['nullable'],'original projected type/NN contract')
    return schema


def parse_field(token,col,nullable):
    if token=='null':
        require(nullable,'original source NOT NULL')
        return None
    typ=col['type']
    if typ=='smallint':
        require(SMALLINT_TEXT.fullmatch(token) is not None,'exact signed SMALLINT grammar')
        value=int(token)
        require(-(1<<15)<=value<(1<<15),'declared SMALLINT range')
        return value
    if typ=='timestamp':checked_timestamp(token)
    elif DECIMAL_TYPE.fullmatch(typ):
        m=DECIMAL_TYPE.fullmatch(typ);decimal_coefficient(token,int(m.group(1)),int(m.group(2)))
    else:
        m=re.fullmatch(r'varchar\(([0-9]+)\)',typ)
        require(m is not None and len(token)<=int(m.group(1)),'declared VARCHAR character bound')
    return token


def typed_rows(data,schema):
    records=data.split(b'\n')
    if records and records[-1]==b'':records.pop()
    rows=[]
    for record in records:
        fields=record.split(b'|')
        require(len(fields)==11,'full11 source field count')
        rows.append([parse_field(raw.decode('utf-8',errors='strict'),col,col['nullable'])
                     for raw,col in zip(fields,schema)])
    return rows


def validate_rows(rows,schema,nullable):
    for row in rows:
        require(len(row)==11,'full11 generated/loaded row count')
        for value,col in zip(row,schema):
            allowed=nullable[0] if col['index']==2 else nullable[1] if col['index']==3 else col['nullable']
            if value is None:require(allowed,'typed fixture NOT NULL');continue
            if col['type']=='smallint':
                require(type(value) is int and -(1<<15)<=value<(1<<15),'full typed SMALLINT')
            else:
                require(type(value) is str,'full typed source lexical field')
                value.encode('utf-8',errors='strict')
                require(parse_field(value,col,allowed)==value,'full typed original source field')


def engine_rows(rows,schema):
    result=[]
    for row in rows:
        typed=[]
        for value,col in zip(row,schema):
            if value is None:typed.append(None)
            elif col['type']=='timestamp':typed.append(checked_timestamp(value))
            elif DECIMAL_TYPE.fullmatch(col['type']):typed.append(Decimal(value))
            else:typed.append(value)
        result.append(typed)
    return result


def oracle(rows):
    # Original datetime.date cast and exact Decimal grouping; no epoch interval,
    # h/l streams, compiled predicates or actual engine outputs are consulted.
    groups=set();selected=[]
    for row in rows:
        passed=row[2] is not None and dt.date(2014,10,17)<=checked_timestamp(row[2]).date()<=dt.date(2015,10,16)
        selected.append(passed)
        if passed:groups.add(None if row[3] is None else Decimal(row[3]))
    coefficients={None if value is None else engine_decimal_coefficient(value,8,4) for value in groups}
    full={'columns':COLUMNS,'rows':[[None if c is None else decimal_text(c,4)] for c in sorted(coefficients,key=key_order)]}
    return full,bitmap(selected)


def result_keys(result):
    require(set(result)=={'columns','rows'} and result['columns']==COLUMNS and type(result['rows']) is list,'complete DECIMAL output schema')
    keys=[]
    for row in result['rows']:
        require(type(row) is list and len(row)==1,'one original output column')
        if row[0] is None:keys.append(None)
        else:
            require(type(row[0]) is str,'exact DECIMAL string interchange')
            coefficient=decimal_coefficient(row[0],8,4)
            require(row[0]==decimal_text(coefficient,4),'canonical fixed4 decimal output')
            keys.append(coefficient)
    require(len(set(keys))==len(keys),'GROUP BY distinct returned keys')
    return keys


def verify_limit_result(result,full):
    expected=set(result_keys(full));actual=result_keys(result)
    require(len(actual)==min(len(expected),130),'exact unordered LIMIT cardinality')
    require(set(actual)<=expected,'all returned LIMIT keys belong to original fullG')
    if len(expected)<=130:require(set(actual)==expected,'full group equality forG<=130')
    return {'status':'passed','returned_rows':len(actual),'complete_group_count':len(expected),
            'contract':'full set equality' if len(expected)<=130 else 'distinct legal130-member subset',
            'original_returned_sequence_preserved':True,'subset_order_not_forced':True}

CASES=['Observed','MixedTail','Limit129','Limit130','Limit131','Limit257',
       'AllExcluded','AllNullTimestamp','Endpoints','Empty','Singleton','Triple']
ROWS=dict(zip(CASES,[65536,1031,129,130,131,1031,63,64,1024,0,1,3]))
NULLABLE_CASES={'MixedTail','AllExcluded','AllNullTimestamp','Singleton','Triple'}
MAPPING=(b'F3_source_column=2\nF3_high_stream=1002\nF3_low_stream=2002\n'
         b'F3_unit=microseconds\nF3_shared_validity=1\nF4_source_column=3\n'
         b'F4_coefficient_stream=3\nF4_scale=4\n')


def fixture_rows(schema,name):
    require(name in CASES and name!='Observed','fixed mathematical case')
    lower=timestamp_us('2014-10-17 00:00:00')
    upper=timestamp_us('2015-10-17 00:00:00')
    mixed_times=[lower-1,lower,lower+1,upper-1,upper,upper+1,-US_DAY-1,-US_DAY,-1,0,1,
                 WORD-1,WORD,WORD+1,timestamp_us('2000-02-29 12:00:00.123456'),None]
    mixed_keys=[None,-99999999,-10000,-1,0,1,10000,99999999]
    endpoint_times=[MIN_US,MIN_US+1,MAX_US-1,MAX_US,-WORD-1,-WORD,-WORD+1,-1,0,1,
        WORD-1,WORD,WORD+1,timestamp_us('1900-02-28 23:59:59.999999'),
        timestamp_us('1900-03-01 00:00:00'),timestamp_us('2000-02-29 12:00:00.123456'),
        timestamp_us('2100-03-01 00:00:00'),lower-1,lower,lower+1,
        upper-US_DAY,upper-US_DAY+43200000000,upper-1,upper,upper+1]
    rows=[]
    for i in range(ROWS[name]):
        row=[]
        for col in schema:
            typ=col['type'];match=DECIMAL_TYPE.fullmatch(typ)
            row.append(0 if typ=='smallint' else decimal_text(0,int(match.group(2))) if match else
                       '1970-01-01 00:00:00.000000' if typ=='timestamp' else '')
        row[5]=row[6]=None if i%3==0 else ''
        integer,key=mixed_times[i%len(mixed_times)],mixed_keys[i%len(mixed_keys)]
        if name in ['Limit129','Limit130','Limit131']:integer,key=lower+i,i-65
        elif name=='Limit257':integer,key=(upper if i%17==0 else lower+i),i%257-128
        elif name=='AllExcluded':integer=[lower-1,upper,None][i%3]
        elif name=='AllNullTimestamp':integer=None
        elif name=='Endpoints':integer,key=endpoint_times[i%len(endpoint_times)],[-99999999,-1,0,1,99999999][i%5]
        elif name=='Singleton':integer,key=lower+1,None
        elif name=='Triple':integer,key=[lower-1,lower,upper][i],[-1,None,1][i]
        row[2]=None if integer is None else restored_timestamp(integer).isoformat(sep=' ',timespec='microseconds')
        row[3]=None if key is None else decimal_text(key,4)
        rows.append(row)
    nullable=name in NULLABLE_CASES
    return rows,(nullable,nullable)


def reconstruct_prepared(folder,rows,binding):
    require((folder/'binding.txt').read_bytes()==(binding+'\n').encode('ascii'),'full source binding')
    require((folder/'component_mapping.txt').read_bytes()==MAPPING,'exact virtual component mapping')
    expected_ts_valid=bitmap([row[2] is not None for row in rows])
    expected_key_valid=bitmap([row[3] is not None for row in rows])
    arrays={}
    for column,expected_valid in [(1002,expected_ts_valid),(2002,expected_ts_valid),(3,expected_key_valid)]:
        values=(folder/f'col{column}.u32').read_bytes()
        require(values==(folder/f'col{column}.biased.u32').read_bytes() and len(values)==4*len(rows),'whole original virtual stream/alias')
        require((folder/f'col{column}.valid.bitmap').read_bytes()==expected_valid,'complete original virtual validity and tail')
        arrays[column]=values
    packed_ts=(folder/'timestamp.i64').read_bytes()
    packed_keys=(folder/'coefficient.i32').read_bytes()
    require(len(packed_ts)==8*len(rows) and len(packed_keys)==4*len(rows),'actual source-width packed lengths')
    require((folder/'timestamp.valid.bitmap').read_bytes()==expected_ts_valid and
            (folder/'coefficient.valid.bitmap').read_bytes()==expected_key_valid,'packed source validity')
    for i,row in enumerate(rows):
        high=struct.unpack_from('<I',arrays[1002],4*i)[0]
        low=struct.unpack_from('<I',arrays[2002],4*i)[0]
        key=struct.unpack_from('<I',arrays[3],4*i)[0]
        stored_ts=struct.unpack_from('<q',packed_ts,8*i)[0]
        stored_key=struct.unpack_from('<i',packed_keys,4*i)[0]
        if row[2] is None:require(high==low==stored_ts==0,'canonical NULL timestamp placeholders')
        else:
            integer=(high-BIAS)*WORD+low
            require(integer==stored_ts and restored_timestamp(integer)==checked_timestamp(row[2]),
                    'full timestamp source reconstruction from high/low versus original datetime')
        if row[3] is None:require(key==stored_key==0,'canonical NULL key placeholders')
        else:
            require(key-BIAS==stored_key and Decimal(decimal_text(stored_key,4))==Decimal(row[3]),
                    'full original exact Decimal coefficient reconstruction')
    require((folder/'empty.txt').exists()==(len(rows)==0),'empty marker corresponds actual empty source')
    if not rows:require((folder/'empty.txt').read_bytes()==b'1\n','exact empty marker')
    return {'status':'passed','rows':len(rows),'logical_projected_values':2*len(rows),
        'physical_component_values':3*len(rows),'full_timestamp_original_value_restoration':True,
        'full_decimal_original_value_restoration':True,'shared_timestamp_validity_and_tails_checked':True,
        'source_width_packed_complete_roundtrip':True,'canonical_null_placeholders_checked':True,
        'empty_source_checked':True,'virtual_columns_are_not_extra_original_columns':True}


def prepare(output):
    require(sha(SQL)==SQL_SHA and sha(DDL)==DDL_SHA,'original raw SQL/DDL identities')
    schema=base_schema()
    data=CSV.read_bytes()
    require(digest(data)=={'bytes':5635711,'sha256':CSV_SHA,'git_sha1':CSV_BLOB},'all original CSV bytes')
    observed=typed_rows(data,schema)
    require(len(observed)==65536,'all original observed source rows')
    output.mkdir(exist_ok=False)
    cases=[]
    for name in CASES:
        folder=output/name;folder.mkdir()
        rows,nullable=(observed,(False,False)) if name=='Observed' else fixture_rows(schema,name)
        require(len(rows)==ROWS[name] and len(rows)<=1000000,'fixed bounded case rows')
        validate_rows(rows,schema,nullable)
        original_jsonl=''.join(json.dumps(row,ensure_ascii=False,separators=(',',':'))+'\n' for row in rows).encode('utf-8')
        binding=CSV_BLOB if name=='Observed' else digest(original_jsonl)['git_sha1']
        write_bytes(folder/'source_rows.jsonl',original_jsonl)
        write_bytes(folder/'binding.txt',(binding+'\n').encode('ascii'))
        write_bytes(folder/'query.sql',SQL.read_bytes())
        ddl=DDL.read_bytes().decode('utf-8',errors='strict')
        if nullable[0]:ddl=ddl.replace('"F3" timestamp NOT NULL','"F3" timestamp')
        if nullable[1]:ddl=ddl.replace('"F4" decimal(8, 4) NOT NULL','"F4" decimal(8, 4)')
        write_bytes(folder/'schema.sql',ddl.encode('utf-8'))
        write_bytes(folder/'component_mapping.txt',MAPPING)
        write_bytes(folder/'columns.tsv',''.join(f'{col} 32 {int(null)} 5 {len(rows)}\n'
                    for col,null in [(1002,nullable[0]),(2002,nullable[0]),(3,nullable[1])]).encode('ascii'))
        ts=[None if row[2] is None else timestamp_us(row[2]) for row in rows]
        keys=[None if row[3] is None else decimal_coefficient(row[3],8,4) for row in rows]
        ts_valid=bitmap([v is not None for v in ts]);key_valid=bitmap([v is not None for v in keys])
        streams={1002:bytearray(),2002:bytearray(),3:bytearray()}
        for integer,coefficient in zip(ts,keys):
            high,low=(0,0) if integer is None else split_timestamp(integer)
            for column,value in [(1002,high),(2002,low),(3,0 if coefficient is None else coefficient+BIAS)]:
                streams[column].extend(struct.pack('<I',value))
        for column in [1002,2002,3]:
            write_bytes(folder/f'col{column}.u32',bytes(streams[column]))
            write_bytes(folder/f'col{column}.biased.u32',bytes(streams[column]))
            write_bytes(folder/f'col{column}.valid.bitmap',ts_valid if column in [1002,2002] else key_valid)
        write_bytes(folder/'timestamp.i64',b''.join(struct.pack('<q',0 if v is None else v) for v in ts))
        write_bytes(folder/'coefficient.i32',b''.join(struct.pack('<i',0 if v is None else v) for v in keys))
        write_bytes(folder/'timestamp.valid.bitmap',ts_valid)
        write_bytes(folder/'coefficient.valid.bitmap',key_valid)
        if not rows:write_bytes(folder/'empty.txt',b'1\n')
        full,selected=oracle(rows)
        write_json(folder/'oracle_full_groups.json',full)
        write_bytes(folder/'oracle.bitmap',selected)
        restore=reconstruct_prepared(folder,rows,binding)
        write_json(folder/'RESTORATION_RECEIPT.json',restore)
        case={'case':name,'observed_original_query':name=='Observed','rows':len(rows),'binding':binding,
            'timestamp_nullable':nullable[0],'coefficient_nullable':nullable[1],'full_schema_columns':11,
            'original_projected_columns':[2,3],'physical_stream_ids':[1002,2002,3],
            'source_csv':str(CSV) if name=='Observed' else None,'source_csv_sha256':CSV_SHA if name=='Observed' else None,
            'source_rows_sha256':sha(folder/'source_rows.jsonl'),'schema_sha256':sha(folder/'schema.sql'),
            'original_ddl_sha256':DDL_SHA,'original_sql_sha256':SQL_SHA,
            'source_schema_variant':'exact original DDL' if name=='Observed' else 'prospective mathematical fixture DDL',
            'complete_group_count':len(full['rows']),'limit_result_cardinality':min(len(full['rows']),130),
            'primary_subset_not_chosen_by_oracle':True,'representation':'lossless timestamp high/low32 + scaled decimal32; fixed family5',
            'source_spelling_and_full_original_bytes_preserved':True,
            'files':{p.name:digest(p.read_bytes()) for p in sorted(folder.iterdir()) if p.is_file()}}
        write_json(folder/'case.json',case);cases.append(case)
    write_json(output/'PREPARATION_RECEIPT.json',{'status':'passed','cases':cases,
        'earlier_full_source_fields_machine_read':True,'fresh_unseen_dataset_claim':False,
        'new_timestamp_decimal_projection_frozen_before_inspection':True,'observed_full_original_rows':65536,
        'no_row_dropping_rounding_or_observed_codec_selection':True,'all_original_source_types_preserved':True,
        'full_timestamp_decimal_source_restoration':True,'fixed_unordered_limit_verifier':True,'no_timing_claim':True})


def selftest_preparer(output):
    schema=base_schema()
    fields=[]
    for col in schema:
        match=DECIMAL_TYPE.fullmatch(col['type'])
        fields.append(b'0' if col['type']=='smallint' else b'1970-01-01 00:00:00' if col['type']=='timestamp' else
                      decimal_text(0,int(match.group(2))).encode('ascii') if match else b'A')
    cases=[('timestamp_not_null',2,b'null'),('coefficient_not_null',3,b'null'),
        ('timestamp_date_only',2,b'2014-10-17'),('timestamp_T_separator',2,b'2014-10-17T00:00:00'),
        ('timestamp_empty_fraction',2,b'2014-10-17 00:00:00.'),
        ('timestamp_excess_fraction',2,b'2014-10-17 00:00:00.0000000'),
        ('timestamp_offset',2,b'2014-10-17 00:00:00+00:00'),('timestamp_Z',2,b'2014-10-17 00:00:00Z'),
        ('timestamp_leap_second',2,b'2014-10-17 00:00:60'),('timestamp_hour24',2,b'2014-10-17 24:00:00'),
        ('timestamp_year0',2,b'0000-01-01 00:00:00'),('timestamp_year10000',2,b'10000-01-01 00:00:00'),
        ('timestamp_infinity',2,b'infinity'),('timestamp_1900_bad_leap',2,b'1900-02-29 00:00:00'),
        ('timestamp_2100_bad_leap',2,b'2100-02-29 00:00:00'),('timestamp_trailing_space',2,b'2014-10-17 00:00:00 '),
        ('decimal_plus',3,b'+1'),('decimal_exponent',3,b'1e0'),('decimal_empty_fraction',3,b'1.'),
        ('decimal_excess_fraction',3,b'0.00000'),('decimal_positive_overflow',3,b'10000'),
        ('decimal_negative_overflow',3,b'-10000'),('decimal_nonfinite',3,b'NaN'),('decimal_whitespace',3,b' 1'),
        ('other_decimal_scale',4,b'0.0000000'),('other_decimal_precision',7,b'1000.000000'),
        ('smallint_overflow',9,b'32768'),('smallint_underflow',10,b'-32769'),
        ('invalid_utf8',0,bytes([255])),('varchar_length',0,b'xxxx')]
    rejected=[]
    for name,column,replacement in cases:
        bad=list(fields);bad[column]=replacement
        try:typed_rows(b'|'.join(bad)+b'\n',schema)
        except (RuntimeError,ValueError,UnicodeDecodeError):rejected.append(name)
        else:raise RuntimeError('missing bounded parser rejection '+name)
    valid_timestamps=(['0001-01-01 00:00:00','9999-12-31 23:59:59.999999',
        '1969-12-31 23:59:59.999999','1970-01-01 00:00:00','2000-02-29 12:00:00.123456']+
        ['2014-10-17 00:00:00.'+'123456'[:n] for n in range(1,7)])
    for token in valid_timestamps:
        require(restored_timestamp(timestamp_us(token))==checked_timestamp(token),'independent valid timestamp endpoint/fraction')
    valid_decimals=['0','-0','000','-000.0000','0.0001','-0.0001','9999.9999','-9999.9999','00001.2','1.2000']
    for token in valid_decimals:
        c=decimal_coefficient(token,8,4)
        require(Decimal(decimal_text(c,4))==Decimal(token),'exact positive decimal spellings')
    require(decimal_coefficient('-0',8,4)==decimal_coefficient('0.0000',8,4)==0,'negative/positive zero coalesce')
    split_cases=[-(1<<63),(1<<63)-1,-WORD-1,-WORD,-WORD+1,-1,0,1,WORD-1,WORD,WORD+1,
                 BIAS-1,BIAS,MIN_US,MAX_US]
    for integer in split_cases:
        h,l=split_timestamp(integer)
        require((h-BIAS)*WORD+l==integer,'all signed64 split endpoints including noncivil extrema')
    # This helper covers the relational verifier, not a fabricated engine result.
    full={'columns':COLUMNS,'rows':[[decimal_text(i,4)] for i in range(257)]}
    legal=[{'columns':COLUMNS,'rows':full['rows'][:130]},
           {'columns':COLUMNS,'rows':list(reversed(full['rows'][-130:]))}]
    for result in legal:verify_limit_result(result,full)
    wrong=[('wrong_member',{'columns':COLUMNS,'rows':full['rows'][:129]+[[decimal_text(999,4)]]},full),
           ('duplicate_member',{'columns':COLUMNS,'rows':full['rows'][:129]+[full['rows'][0]]},full),
           ('short129',{'columns':COLUMNS,'rows':full['rows'][:129]},full),
           ('long131',{'columns':COLUMNS,'rows':full['rows'][:131]},full),
           ('smallG_missing',{'columns':COLUMNS,'rows':full['rows'][:128]}, {'columns':COLUMNS,'rows':full['rows'][:129]})]
    rejected_results=[]
    for name,result,expected in wrong:
        try:verify_limit_result(result,expected)
        except RuntimeError:rejected_results.append(name)
        else:raise RuntimeError('missing bounded LIMIT verifier rejection '+name)
    write_json(output,{'preparer_helper_fixtures':'PASS','parser_rejections':rejected,
        'valid_timestamp_cases':len(valid_timestamps),'valid_decimal_cases':len(valid_decimals),
        'signed64_split_cases':len(split_cases),'different_legal_limit_subsets':len(legal),
        'limit_result_rejections':rejected_results,'observed_values_used':False})


def duckdb_identity():
    require(sha(DUCK_NATIVE)==DUCK_SHA,'pinned actual DuckDB native identity')
    sys.path.insert(0,str(DUCK_NATIVE.parent))
    import duckdb
    import _duckdb
    require(duckdb.__version__=='2.0.0' and Path(_duckdb.__file__).resolve()==DUCK_NATIVE.resolve(),'actual pinned DuckDB version/file')
    return duckdb,{'version':duckdb.__version__,'python_module':duckdb.__file__,
        'python_module_sha256':sha(Path(duckdb.__file__)),'native_module':_duckdb.__file__,
        'native_module_sha256':sha(Path(_duckdb.__file__)),'cpu_affinity':sorted(os.sched_getaffinity(0)),
        'environment_threads':{k:os.environ.get(k) for k in ['OMP_NUM_THREADS','OPENBLAS_NUM_THREADS','MKL_NUM_THREADS','RAYON_NUM_THREADS']}}


def session_settings(con):
    con.execute('SET threads=1').fetchall()
    con.execute('SET preserve_insertion_order=true').fetchall()
    settings={k:con.execute(f"SELECT current_setting('{k}')").fetchall()[0][0]
              for k in ['threads','default_collation','preserve_insertion_order','TimeZone']}
    settings['binary_behavior_probes']=list(con.execute("SELECT 'A'='a','é'='e','é'='é'").fetchall()[0])
    require(settings['threads']==1 and settings['default_collation'] in ('','binary') and
            settings['preserve_insertion_order'] is True and settings['binary_behavior_probes']==[False,False,False],
            'actual single-thread binary session; timestamp is naive not timezone converted')
    return settings


def checked_case(folder):
    meta=read_json(folder/'case.json')
    require(meta['case'] in CASES and meta['rows']==ROWS[meta['case']] and
            sha(folder/'query.sql')==SQL_SHA and sha(folder/'schema.sql')==meta['schema_sha256'],
            'exact prepared case/SQL/schema')
    nullable=meta['case'] in NULLABLE_CASES
    require(meta['timestamp_nullable'] is nullable and meta['coefficient_nullable'] is nullable,'prospective exact fixture nullability')
    if meta['case']=='Observed':require(meta['schema_sha256']==DDL_SHA,'observed exact original full11 DDL')
    return meta


def duckdb_build(folder,storage):
    began=time.perf_counter();duckdb,identity=duckdb_identity();meta=checked_case(folder)
    require(sha(folder/'source_rows.jsonl')==meta['source_rows_sha256'],'all11 typed source JSONL identity')
    raw=(folder/'source_rows.jsonl').read_bytes().decode('utf-8',errors='strict')
    require(not raw or raw.endswith('\n'),'source JSONL final LF')
    lexical=[json.loads(line) for line in raw.split('\n')[:-1]] if raw else []
    schema=base_schema();require(len(lexical)==meta['rows'],'all original engine rows')
    validate_rows(lexical,schema,(meta['timestamp_nullable'],meta['coefficient_nullable']))
    rows=engine_rows(lexical,schema)
    source_read_ms=(time.perf_counter()-began)*1000
    storage.mkdir(exist_ok=False);database=storage/'full11.duckdb'
    con=duckdb.connect(str(database),config={'threads':'1','preserve_insertion_order':'true'})
    settings=session_settings(con);start=time.perf_counter()
    con.execute((folder/'schema.sql').read_bytes().decode('utf-8',errors='strict')).fetchall()
    if rows:
        con.execute('BEGIN TRANSACTION').fetchall()
        con.executemany('INSERT INTO "Arade_1" VALUES ('+','.join(['?']*11)+')',rows).fetchall()
        con.execute('COMMIT').fetchall()
    columns=con.execute("PRAGMA table_info('Arade_1')").fetchall()
    require(len(columns)==len(schema)==11,'actual full11 engine schema')
    for i,(actual,col) in enumerate(zip(columns,schema)):
        expected_type='VARCHAR' if col['type'].startswith('varchar(') else col['type'].replace(' ','').upper()
        nullable=meta['timestamp_nullable'] if i==2 else meta['coefficient_nullable'] if i==3 else col['nullable']
        require(actual[0]==i and actual[1]==col['name'] and actual[2]==expected_type and bool(actual[3]) is (not nullable),
                'all11 actual names/types/order/nullability')
    con.execute('CHECKPOINT').fetchall();con.close();build_ms=(time.perf_counter()-start)*1000
    con=duckdb.connect(str(database),read_only=True,config={'threads':'1','preserve_insertion_order':'true'})
    session_settings(con)
    fetched=con.execute('SELECT rowid,* FROM "Arade_1" ORDER BY rowid').fetchall()
    require(len(fetched)==len(rows),'full persisted engine original row count')
    for i,(actual,expected) in enumerate(zip(fetched,rows)):
        require(len(actual)==12 and actual[0]==i and
                all(type(a) is type(b) and a==b for a,b in zip(actual[1:],expected)),
                'every full11 typed timestamp/Decimal/string/SMALLINT value restored')
    con.close()
    write_json(storage/'build_receipt.json',{'status':'passed','method':'duckdb','case':meta['case'],
        'identity':identity,'settings':settings,'rows':len(rows),'full_value_cells_verified':11*len(rows),
        'schema_columns':columns,'timestamp_columns_typed_roundtripped':[2],
        'decimal_columns_typed_roundtripped':[3,4,7,8],'source_read_and_identity_ms':source_read_ms,
        'literal_ddl_insert_checkpoint_ms':build_ms,'build_save_validate_ms':(time.perf_counter()-began)*1000,
        'full11_database_bytes':database.stat().st_size,'database_sha256':sha(database),
        'source_rows_sha256':meta['source_rows_sha256'],'original_sql_sha256':SQL_SHA,'schema_sha256':meta['schema_sha256'],
        'ingestion':'checked full11 typed source rows via executemany; shared parser, not independent CSV parsing',
        'source_parse_and_native_preparation_cost_separate':True,'fulltable_vs_projected_compression_ratio_not_defined':True,
        'peak_process_rss_kib':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,'no_timing_claim':True})


def engine_result(cursor):
    columns=[{'name':entry[0],'type':str(entry[1])} for entry in cursor.description]
    raw=cursor.fetchall()
    require(columns==COLUMNS,'actual original F4 DECIMAL(8,4) result metadata')
    rows=[]
    for entry in raw:
        require(len(entry)==1,'original one-column output')
        value=entry[0]
        rows.append([None if value is None else decimal_text(engine_decimal_coefficient(value,8,4),4)])
    result={'columns':columns,'rows':rows}
    result_keys(result)
    return result


def duckdb_query(folder,storage,sqlpath,resultpath,bitmappath,fullpath,metricspath):
    began=time.perf_counter();duckdb,identity=duckdb_identity();meta=checked_case(folder)
    receipt=read_json(storage/'build_receipt.json');database=storage/'full11.duckdb'
    require(receipt['status']=='passed' and receipt['case']==meta['case'] and
            receipt['source_rows_sha256']==meta['source_rows_sha256'] and sha(database)==receipt['database_sha256'] and
            sha(sqlpath)==SQL_SHA,'actual engine stored input identities')
    diagnostic=bitmappath!='-'
    require(diagnostic==(fullpath!='-'),'paired fullG and bitmap diagnostic contract')
    con=duckdb.connect(str(database),read_only=True,config={'threads':'1','preserve_insertion_order':'true'})
    settings=session_settings(con);setup_ms=(time.perf_counter()-began)*1000
    sql=sqlpath.read_bytes().decode('utf-8',errors='strict')
    start=time.perf_counter();result=engine_result(con.execute(sql))
    execute_fetch_ms=(time.perf_counter()-start)*1000
    # Preserve the full actual original returned sequence; do not sort/choose a
    # replacement subset. No oracle or source values are read by this worker.
    write_json(resultpath,result)
    diagnostic_ms=0.0;bitmap_bytes=0;full_bytes=0;full_groups=None
    if diagnostic:
        tick=time.perf_counter()
        require(sql.endswith(' LIMIT 130;\n'),'exact original suffix for labeled auxiliary noLIMIT')
        auxiliary_sql=sql[:-len(' LIMIT 130;\n')]+';\n'
        full=engine_result(con.execute(auxiliary_sql))
        full['rows'].sort(key=lambda row:key_order(None if row[0] is None else decimal_coefficient(row[0],8,4)))
        write_json(Path(fullpath),full);full_bytes=Path(fullpath).stat().st_size;full_groups=len(full['rows'])
        selected=con.execute('SELECT rowid, ((CAST("F3" AS DATE)>=CAST(\'2014-10-17\' AS DATE)) AND '
            '(CAST("F3" AS DATE)<=CAST(\'2015-10-16\' AS DATE))) FROM "Arade_1" ORDER BY rowid').fetchall()
        require(len(selected)==meta['rows'] and all(row[0]==i for i,row in enumerate(selected)),'full diagnostic original row ids')
        data=bitmap([row[1] is True for row in selected]);write_bytes(Path(bitmappath),data);bitmap_bytes=len(data)
        diagnostic_ms=(time.perf_counter()-tick)*1000
    con.close()
    write_json(metricspath,{'method':'duckdb','case':meta['case'],'identity':identity,'settings':settings,
        'rows':meta['rows'],'actual_cursor_description':result['columns'],'original_engine_returned_sequence':result['rows'],
        'primary_original_SQL_executions':1,'primary_result_reordered_or_subset_forced':False,
        'setup_ms':setup_ms,'execute_fetch_ms':execute_fetch_ms,'result_bytes':resultpath.stat().st_size,
        'diagnostic_enabled':diagnostic,'diagnostic_ms_separate':diagnostic_ms,'diagnostic_bitmap_bytes':bitmap_bytes,
        'diagnostic_full_groups_bytes':full_bytes,'diagnostic_full_group_count':full_groups,
        'diagnostic_noLIMIT_is_auxiliary_not_primary':diagnostic,'diagnostic_auxiliary_query_count':2 if diagnostic else 0,
        'query_worker_does_not_read_source_rows_or_oracle':True,'full_worker_ms_before_metrics':(time.perf_counter()-began)*1000,
        'peak_process_rss_kib':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,'no_timing_claim':True})


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('mode',choices=['prepare','selftest_preparer','identity','duckdb_build','duckdb_query'])
    parser.add_argument('paths',nargs='+');args=parser.parse_args()
    if args.mode=='prepare':require(len(args.paths)==1,'prepare arity');prepare(Path(args.paths[0]))
    elif args.mode=='selftest_preparer':require(len(args.paths)==1,'helper arity');selftest_preparer(Path(args.paths[0]))
    elif args.mode=='identity':
        require(len(args.paths)==1,'identity arity');_,identity=duckdb_identity();write_json(Path(args.paths[0]),identity)
    elif args.mode=='duckdb_build':require(len(args.paths)==2,'engine build arity');duckdb_build(*map(Path,args.paths))
    else:
        require(len(args.paths)==7,'engine query arity')
        folder,storage,sqlpath,result,bitmap_path,fullpath,metrics=args.paths
        duckdb_query(Path(folder),Path(storage),Path(sqlpath),Path(result),bitmap_path,fullpath,Path(metrics))


if __name__=='__main__':main()
