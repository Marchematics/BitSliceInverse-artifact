#!/usr/bin/env python3
"""Complete exact IG/2 source-qualified gate; no performance panel or automatic retry.

Seven fixed datasets (one actual source, six mathematical variants), six native
paths and actual full18-column DuckDB. Every primary and optional-diagnostic
execution returns every typed aggregate tuple. No old frozen file is edited.
"""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parent))
import artifact_paths as _artifact
import argparse
import datetime as dt
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parent
WS = ROOT.parents[1]
FREEZE = 'FULL_SQL_IG2_V1R1_SOURCE_FREEZE.json'
CASES = ['Observed', 'MixedTail', 'AbsentLiteral', 'AllExcluded', 'AllNullPayload', 'Endpoints', 'Empty']
ROWS = dict(zip(CASES, [65536, 1031, 65, 63, 64, 1024, 0]))
METHODS = ['generic', 'expert', 'scan_fused', 'group_then_filter', 'cache_fused', 'packed_code_fused']
SQL_REL = 'source/PublicBI_fullsql/benchmark__IGlocations1__queries__2.sql'
DDL_REL = 'source/PublicBI_fullsql/benchmark__IGlocations1__tables__IGlocations1_1.table.sql'
SQL_SHA = '0bc95092f92bbb2f8652e66c10db8bec5fba88b6d96c5ac23e9650fe27ecca8b'
DDL_SHA = 'bd070cf6fed9e82b84ea20636b13ab014cb69970892dc452c8039f96611da0bc'
CSV = ROOT / 'input_round3/IGlocations1_1.csv'
CSV_SHA = '186979a35563c6b6a7e88b21b5385e4ed70d131b852770d0c2423c2a76c2a447'
ALIAS = 'TEMP(Calculation_8840730155303010)(3219150654)(0)'
DISK_RESERVE_BYTES = 512*1024*1024
PROJECTED_OUTPUT_BOUND_BYTES = 64*1024*1024
RESULT_COLUMNS = [{'name': 'State', 'type': 'VARCHAR'}, {'name': ALIAS, 'type': 'HUGEINT'}]


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def utc():
    return dt.datetime.now(dt.timezone.utc).isoformat()


def sha(path):
    h = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b''):
            h.update(chunk)
    return h.hexdigest()


def fingerprint(path):
    s = path.stat()
    return [s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns]


def read_json(path):
    return json.loads(path.read_text(encoding='utf-8'))


def write_json(path, value):
    with path.open('x', encoding='utf-8', newline='\n') as stream:
        json.dump(value, stream, indent=2, ensure_ascii=False, allow_nan=False)
        stream.write('\n')
        stream.flush()
        os.fsync(stream.fileno())


def json_lines(path):
    result = []
    for line in path.read_text().splitlines():
        try:
            value = json.loads(line)
            if isinstance(value, dict):
                result.append(value)
        except json.JSONDecodeError:
            pass  # compiler/helper diagnostics remain retained verbatim
    return result


def finite_nonnegative(value):
    return type(value) in (int, float) and math.isfinite(value) and value >= 0


def exact_result(path, expected):
    result = read_json(path)
    require(set(result) == {'columns', 'rows'} and result['columns'] == RESULT_COLUMNS, 'complete result schema')
    require(isinstance(result['rows'], list), 'complete result rows')
    keys = []
    for row in result['rows']:
        require(isinstance(row, list) and len(row) == 2 and type(row[0]) is str, 'exact result key')
        require(row[1] is None or (type(row[1]) is str and str(int(row[1])) == row[1] and
                                  -(1 << 127) <= int(row[1]) < (1 << 127)), 'canonical signed128 result value')
        keys.append(row[0].encode('utf-8', errors='strict'))
    require(keys == sorted(keys) and len(set(keys)) == len(keys), 'canonical complete unordered GROUP BY result')
    require(result == expected, 'all exact original-string/wide-sum tuples differ from independent oracle')
    return {'path': str(path), 'sha256': sha(path), 'bytes': path.stat().st_size,
            'full_rows_compared': len(result['rows']), 'exact_typed_tuples_equal': True}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--cpu-granted', action='store_true')
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--cap-seconds', type=int, default=300)
    args = parser.parse_args()
    require(args.cpu_granted and args.run_id.isidentifier() and 0 < args.cap_seconds <= 300,
            'explicit parent grant, fresh identifier and <=300s cap required')
    lock = (WS / 'benchmark_cpu.lock').open('a+')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    out = ROOT / 'results' / args.run_id
    out.mkdir(exist_ok=False)
    began, start = utc(), time.monotonic()
    deadline = start + args.cap_seconds
    journal = (out / 'journal.jsonl').open('x', buffering=1)
    comparisons = (out / 'comparisons.jsonl').open('x', buffering=1)
    commands, answers, values, helper_receipts, rejections, representations = [], [], [], [], [], []
    source_ids, input_ids, binary_ids, immutable_stats = {}, {}, {}, {}
    status, error = 'failed', None
    env = os.environ.copy()
    env['PYTHONDONTWRITEBYTECODE'] = '1'
    env.update({key: '1' for key in ['OMP_NUM_THREADS', 'OPENBLAS_NUM_THREADS', 'MKL_NUM_THREADS',
               'NUMEXPR_NUM_THREADS', 'RAYON_NUM_THREADS', 'TOKIO_WORKER_THREADS']})

    def expire(signum, frame):
        raise TimeoutError('full SQL IG/2 whole gate cap expired')

    signal.signal(signal.SIGALRM, expire)

    def bind(path, expected, target):
        path = path.resolve()
        key = str(path)
        require(key not in target or target[key] == expected, f'conflicting file identity: {key}')
        require(sha(path) == expected, f'file identity: {key}')
        target[key] = expected
        immutable_stats[key] = fingerprint(path)

    def inventory(paths):
        files = [{'path': str(path), 'bytes': path.stat().st_size, 'sha256': sha(path)} for path in paths]
        require(len({row['path'] for row in files}) == len(files), 'representation files counted once')
        return {'files': files, 'total_bytes': sum(row['bytes'] for row in files)}

    def run(name, cmd, cap=60, expected_code=0):
        require(all(fingerprint(Path(p)) == stat for p, stat in immutable_stats.items()), 'source/input/binary changed before worker')
        remaining = deadline - time.monotonic()
        require(remaining > 0, 'whole stage deadline')
        tick, begin = time.perf_counter(), utc()
        process, code, interrupted = None, None, None
        with (out / f'{name}.stdout').open('xb') as so, (out / f'{name}.stderr').open('xb') as se:
            try:
                previous = signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGALRM})
                try:
                    process = subprocess.Popen([sys.executable, str(ROOT / 'process_receipt.py'),
                        str(out / f'{name}.resource.json'), *map(str, cmd)], cwd=ROOT, env=env,
                        stdout=so, stderr=se, start_new_session=True,
                        preexec_fn=lambda: signal.pthread_sigmask(signal.SIG_SETMASK, previous))
                finally:
                    signal.pthread_sigmask(signal.SIG_SETMASK, previous)
                code = process.wait(timeout=min(cap, remaining))
            except BaseException as exc:
                interrupted = exc
                signal.setitimer(signal.ITIMER_REAL, 0)
                if process is not None:
                    try:
                        os.killpg(process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    code = process.wait()
        receipt_path = out / f'{name}.resource.json'
        record = {'name': name, 'command': list(map(str, cmd)), 'start_utc': begin,
            'end_utc': utc(), 'wall_s': time.perf_counter()-tick, 'exit_code': code,
            'expected_exit_code': expected_code, 'timed_out': isinstance(interrupted, (TimeoutError, subprocess.TimeoutExpired)),
            'interrupted': interrupted is not None, 'pid': process.pid if process else None,
            'reaped': process is not None and process.returncode is not None,
            'worker_reaped_receipt': receipt_path.exists()}
        commands.append(record)
        journal.write(json.dumps(record)+'\n')
        journal.flush()
        os.fsync(journal.fileno())
        if interrupted is not None:
            raise interrupted
        require(code == expected_code and receipt_path.exists(), f'{name} failed or resource receipt missing')
        require(shutil.disk_usage(out).free >= DISK_RESERVE_BYTES, 'shared512MiB reserve crossed; retain partial evidence and stop')
        receipt = read_json(receipt_path)
        require(receipt['exit_code'] == expected_code and finite_nonnegative(receipt['wall_s'])
                and type(receipt['max_rss_kib']) is int and receipt['max_rss_kib'] > 0,
                f'{name} whole-worker receipt')
        return record

    try:
        signal.setitimer(signal.ITIMER_REAL, max(.001, deadline-time.monotonic()))
        os.sched_setaffinity(0, {min(os.sched_getaffinity(0))})
        cpu = min(os.sched_getaffinity(0))
        proc, proc_error = '', None
        try:
            proc = Path('/proc/cpuinfo').read_text()
        except OSError as exc:
            proc_error = str(exc)
        records = [{k.strip(): v.strip() for k, v in (line.split(':', 1) for line in block.splitlines() if ':' in line)}
                   for block in proc.split('\n\n') if block.strip()]
        selected = next((record for record in records if record.get('processor') == str(cpu)), {})
        flags = selected.get('flags', '').split()
        sse2 = 'sse2' in flags if flags else os.uname().machine.lower() in ('x86_64', 'amd64')
        write_json(out / 'CPU_IDENTITY.json', {'uname': list(os.uname()), 'affinity': [cpu],
            'selected_proc_cpuinfo_record': selected, 'proc_error': proc_error, 'sse2_precheck': sse2,
            'precheck_source': 'proc flags' if flags else 'x86-64 ABI', 'native_runtime_check_required': True})
        require(sse2, 'SSE2 unavailable; no silent substitution')
        freeze = read_json(ROOT / FREEZE)
        required = {'prepare_fullsql_ig2.py', 'run_fullsql_ig2_gate.py', 'run_fullsql_ig2_gate_v1r1.py', 'PROTOCOL_FULL_SQL_IG2_V1R1.md', 'REVIEW_FULL_SQL_IG2_V1R1_RECEIPTS.md', 'PROTOCOL_FULL_SQL_IG2.md',
            'SOURCE_REVIEW_CHECKLIST_FULL_SQL_IG2.md', 'REVIEW_FULL_SQL_IG2_SOURCE.md',
            'SOURCE_ASSESSMENT_FULL_SQL.md', 'METHOD_FULL_SQL_IG2_SEMANTICS.md',
            'SOURCE_NOTES_DUCKDB_IG2.md', 'FULL_SQL_COVERAGE_PRE_GATE.json', 'src_fullsql_ig2/adapter.cpp',
            'src_patch_fusion/fullsql_reference.hpp', 'src_patch_fusion/adapter.cpp', 'process_receipt.py',
            'COHORT_ROUND3.json', 'PATCH_FUSION_SOURCE_FREEZE.json', SQL_REL, DDL_REL,
            'source/PublicBI_fullsql/SOURCE_MANIFEST.json'}
        require(required <= set(freeze['files']), 'new source freeze missing required source/qualification')
        for name in ['COHORT_SOURCE_FREEZE.json', 'FACTORIAL_SOURCE_FREEZE.json', 'DECODE_ONCE_SOURCE_FREEZE.json',
                     'BANK_SHARING_SOURCE_FREEZE.json', 'PATCH_FUSION_SOURCE_FREEZE.json', FREEZE]:
            prior = read_json(ROOT / name)
            for relative, expected in prior['files'].items():
                path = ROOT / relative
                require(path.resolve().is_relative_to(ROOT.resolve()), 'source freeze path outside research ROOT')
                bind(path, expected, source_ids)
            bind(ROOT / name, sha(ROOT / name), source_ids)
        for folder in ['src_fullsql_ig2', 'vendor/src']:
            require(all(str(p.resolve()) in source_ids for p in (ROOT / folder).rglob('*') if p.is_file()), 'unfrozen compilation input')
        old = (ROOT / 'src_patch_fusion/adapter.cpp').read_bytes()
        reference = (ROOT / 'src_patch_fusion/fullsql_reference.hpp').read_bytes()
        old_main = b'int main(int argc,char**argv)'
        new_main = b'static int frozen_patch_main(int argc,char**argv)'
        require(old.count(old_main) == reference.count(new_main) == 1 and
                reference.replace(new_main, old_main, 1) == old, 'frozen compiler differs beyond sole entrypoint rename')
        require(sha(ROOT / SQL_REL) == SQL_SHA and sha(ROOT / DDL_REL) == DDL_SHA, 'exact original SQL/DDL')
        # The new source set is completely bound before this new projected input read.
        bind(CSV, CSV_SHA, input_ids)
        write_json(out / 'DISK_PREFLIGHT.json', {'free_bytes': shutil.disk_usage(out).free,
            'shared_reserve_bytes': DISK_RESERVE_BYTES, 'projected_output_bound_bytes': PROJECTED_OUTPUT_BOUND_BYTES,
            'required_free_bytes': DISK_RESERVE_BYTES+PROJECTED_OUTPUT_BOUND_BYTES})
        require(shutil.disk_usage(out).free >= DISK_RESERVE_BYTES+PROJECTED_OUTPUT_BOUND_BYTES,
                'preserve512MiB shared reserve plus64MiB projected fresh output headroom')
        write_json(out / 'SOURCE_IDENTITIES.json', source_ids)
        shutil.copyfile(ROOT / FREEZE, out / FREEZE)
        worker = ROOT / 'prepare_fullsql_ig2.py'
        run('compiler_version', ['g++', '--version'], 10)
        vendor = sorted((ROOT / 'vendor/src').rglob('*.cpp'))
        common = ['g++', '-std=c++20', '-I'+str(ROOT / 'vendor/src/include'),
                  str(ROOT / 'src_fullsql_ig2/adapter.cpp'), *map(str, vendor)]
        for build in ['normal', 'ubsan']:
            flags = ['-O2', '-Wall', '-Wextra'] if build == 'normal' else ['-O1', '-g0', '-fsanitize=undefined', '-fno-sanitize-recover=all']
            binary = out / f'adapter_{build}'
            run(f'compile_{build}', [*common, *flags, '-o', binary], 120)
            bind(binary, sha(binary), binary_ids)
            run(f'selftest_{build}', [binary, 'selftest_fullsql'], 15)
            helper = [row for row in json_lines(out / f'selftest_{build}.stdout') if 'fullsql_helper_fixtures' in row]
            require(len(helper) == 1 and helper[0]['fullsql_helper_fixtures'] == 'PASS' and
                    helper[0]['observed_values_used'] is False, 'native helper assertions')
            helper_receipts.append({'build': build, **helper[0]})
        write_json(out / 'BINARY_IDENTITIES.json', binary_ids)
        run('duckdb_identity', [sys.executable, worker, 'identity', out / 'DUCKDB_IDENTITY.json'], 20)
        api_id = read_json(out / 'DUCKDB_IDENTITY.json')
        require(api_id['version'] == '2.0.0', 'actual pinned external engine')
        bind(Path(api_id['native_module']), api_id['native_module_sha256'], input_ids)
        bind(Path(api_id['python_module']), api_id['python_module_sha256'], input_ids)
        run('selftest_preparer', [sys.executable, worker, 'selftest_preparer', out / 'PREPARER_HELPER_RECEIPT.json'], 20)
        parser_helper = read_json(out / 'PREPARER_HELPER_RECEIPT.json')
        require(parser_helper == {'preparer_helper_fixtures': 'PASS',
            'rejections': ['state_not_null', 'population_not_null', 'positive_int32_overflow',
                           'negative_int32_underflow', 'invalid_utf8', 'varchar_character_bound'],
            'valid_endpoint_rows': 2, 'observed_values_used': False}, 'bounded parser helper fixture receipt')
        prepared = out / 'prepared'
        run('prepare', [sys.executable, worker, 'prepare', prepared], 60)
        prep = read_json(prepared / 'PREPARATION_RECEIPT.json')
        require(prep['status'] == 'passed' and [case['case'] for case in prep['cases']] == CASES and
                prep['full_dictionary_original_value_restoration'] is True and
                prep['fresh_unseen_dataset_claim'] is False, 'complete prepared fixed roster')
        for path in sorted(prepared.rglob('*')):
            if path.is_file():
                bind(path, sha(path), input_ids)
        bad_sql = out / 'rejected_modified_query.sql'
        bad_sql.write_bytes((ROOT / SQL_REL).read_bytes()+b' ')  # no alternative workload execution
        bind(bad_sql, sha(bad_sql), input_ids)
        for case in CASES:
            folder = prepared / case
            meta = read_json(folder / 'case.json')
            expected = read_json(folder / 'oracle.json')
            expected_bitmap = (folder / 'oracle.bitmap').read_bytes()
            require(meta['rows'] == ROWS[case] and len(expected_bitmap) == 8*((ROWS[case]+63)//64), 'fixed complete rows/bitmap')
            restoration = read_json(folder / 'RESTORATION_RECEIPT.json')
            require(restoration['status'] == 'passed' and restoration['projected_values'] == 2*ROWS[case]
                    and restoration['dictionary_strings_reconstructed_against_original_source'] is True
                    and restoration['complete_validity_and_null_placeholders_checked'] is True
                    and restoration['empty_source_checked'] is True, 'source-string/full validity reconstruction')
            encoded_ids = []
            for build in ['normal', 'ubsan']:
                binary = out / f'adapter_{build}'
                encoded = out / f'{case}_{build}.bsql'
                run(f'{case}_{build}_build', [binary, 'build_fullsql', folder, encoded], 30)
                bind(encoded, sha(encoded), input_ids)
                encoded_ids.append(sha(encoded))
                run(f'{case}_{build}_verify', [binary, 'verify_fullsql', encoded, folder], 30)
                verify_stdout = json_lines(out / f'{case}_{build}_verify.stdout')
                if ROWS[case]:
                    reconstructed = [row for row in verify_stdout if 'checked_nullable_logical_rows' in row]
                    require(len(reconstructed) == 1 and reconstructed[0]['checked_nullable_logical_rows'] == 2*ROWS[case], 'complete native vector verification')
                else:
                    require(encoded.stat().st_size == 48 and not expected['rows'] and not expected_bitmap,
                            'empty encoded source/result contract')
                values.append({'case': case, 'build': build, 'full_projected_values': 2*ROWS[case],
                    'complete_vectors_and_validity_checked': True, 'stdout': verify_stdout,
                    'encoded_path': str(encoded), 'encoded_bytes': encoded.stat().st_size, 'encoded_sha256': sha(encoded),
                    'dictionary_bytes_additional': (folder / 'state.dict').stat().st_size})
                for method in METHODS:
                    for diagnostic in [False, True]:
                        name = f'{case}_{build}_{method}_{"diagnostic" if diagnostic else "primary"}'
                        result, metrics, bm = out / f'{name}.result.json', out / f'{name}.metrics.jsonl', out / f'{name}.bitmap'
                        run(name, [binary, 'query_fullsql', encoded, folder, folder / 'query.sql', method,
                                   result, bm if diagnostic else '-', metrics], 30)
                        compared = exact_result(result, expected)
                        lines = json_lines(metrics)
                        require(len(lines) == 1, 'exact one native metrics record')
                        metric = lines[0]
                        require(metric['method'] == method and metric['rows'] == ROWS[case]
                            and metric['diagnostic_enabled'] is diagnostic and metric['empty_relation'] is (ROWS[case] == 0)
                            and metric['full_result_serialization_charged'] is True and metric['sum_type'] == 'HUGEINT'
                            and metric['runtime_sse2'] is True and
                            metric['group_key_filter_after_aggregate'] is (method == 'group_then_filter'), 'native output contract')
                        for field in ['setup_ms', 'query_ms', 'plan_ms', 'cold_decode_once_ms', 'output_write_ms',
                                      'selection_bitmap_bytes', 'diagnostic_bitmap_bytes', 'plan_owned_capacity_bytes',
                                      'aggregate_capacity_bytes', 'code_cache_capacity_bytes', 'decoder_scratch_nominal_bytes',
                                      'payload_and_key_decode_blocks', 'predicate_decode_blocks', 'whole_process_rss_kib']:
                            require(finite_nonnegative(metric[field]), f'native lifecycle/capacity field {field}')
                        require(metric['result_bytes'] == result.stat().st_size and
                            metric['selection_bitmap_bytes'] == (len(expected_bitmap) if method in ['generic', 'expert'] else 0)
                            and metric['diagnostic_bitmap_bytes'] == (len(expected_bitmap) if diagnostic else 0)
                            and metric['compressed_tree_retained'] is (method != 'packed_code_fused' and ROWS[case] > 0),
                            'native actual representation/bitmap contract')
                        if diagnostic:
                            require(bm.read_bytes() == expected_bitmap, 'complete native selection bitmap equality')
                        else:
                            require(not bm.exists(), 'primary scan not forced to write diagnostic bitmap')
                        answer = {'case': case, 'build': build, 'method': method, 'diagnostic': diagnostic,
                            'result': compared, 'metrics_path': str(metrics), 'metrics': metric,
                            'bitmap_path': str(bm) if diagnostic else None,
                            'bitmap_sha256': sha(bm) if diagnostic else None,
                            'complete_bitmap_equal': diagnostic, 'no_timing_claim': True}
                        answers.append(answer)
                        comparisons.write(json.dumps(answer)+'\n')
                if case == 'Observed':
                    name = f'reject_modified_sql_{build}'
                    run(name, [binary, 'query_fullsql', encoded, folder, bad_sql, 'generic',
                               out / f'{name}.result', '-', out / f'{name}.metrics'], 15, expected_code=2)
                    require('only exact pinned IG/2 SQL admitted' in (out / f'{name}.stderr').read_text() and
                            not (out / f'{name}.result').exists() and not (out / f'{name}.metrics').exists(),
                            'changed SQL must reject before result')
                    rejections.append({'build': build, 'case': 'modified SQL extra space', 'exit_code': 2})
            require(len(set(encoded_ids)) == 1, 'normal/UBSan encoded representations differ')
            storage = out / f'duckdb_{case}'
            run(f'duckdb_{case}_build', [sys.executable, worker, 'duckdb_build', folder, storage], 90)
            engine_build = read_json(storage / 'build_receipt.json')
            require(engine_build['status'] == 'passed' and engine_build['rows'] == ROWS[case] and
                    engine_build['full_value_cells_verified'] == 18*ROWS[case] and
                    engine_build['schema_sha256'] == meta['schema_sha256'] and
                    engine_build['fulltable_vs_projected_compression_ratio_not_defined'] is True,
                    'actual engine full18 roundtrip')
            bind(storage / 'full18.duckdb', engine_build['database_sha256'], input_ids)
            bind(storage / 'build_receipt.json', sha(storage / 'build_receipt.json'), input_ids)
            common_files = [folder / 'state.dict', folder / 'binding.txt']
            if ROWS[case] == 0:
                common_files.append(folder / 'empty.txt')
            compressed = {build: inventory([out / f'{case}_{build}.bsql', *common_files])
                          for build in ['normal', 'ubsan']}
            if ROWS[case] == 0:
                packed_files = [out / f'{case}_normal.bsql', *common_files]
            else:
                packed_files = [folder / f'col{col}{suffix}' for col in [17, 13]
                                for suffix in ['.u32', '.valid.bitmap']]
                packed_files += [folder / 'columns.tsv', *common_files]
            representations.append({'case': case, 'rows': ROWS[case],
                'native_compressed_projected2_required': compressed,
                'packed_code_projected2_required': inventory(packed_files),
                'actual_duckdb_full18_database': inventory([storage / 'full18.duckdb']),
                'actual_duckdb_attestation_and_bridge_metadata_additional': inventory([
                    storage / 'build_receipt.json', folder / 'case.json', folder / 'schema.sql']),
                'shared_original_query_input': inventory([folder / 'query.sql']),
                'all_prepared_evidence_physical_files': inventory(sorted(p for p in folder.iterdir() if p.is_file())),
                'biased_aliases_not_counted_twice_as_production_format': True,
                'preparation_and_evidence_bytes_not_silently_omitted': True,
                'different_projection_scopes_no_compression_ratio': True,
                'serialized_bytes_not_resident_memory': True})
            for diagnostic in [False, True]:
                name = f'duckdb_{case}_{"diagnostic" if diagnostic else "primary"}'
                result, metrics, bm = out / f'{name}.result.json', out / f'{name}.metrics.json', out / f'{name}.bitmap'
                run(name, [sys.executable, worker, 'duckdb_query', folder, storage, folder / 'query.sql',
                           result, bm if diagnostic else '-', metrics], 30)
                compared, metric = exact_result(result, expected), read_json(metrics)
                require(metric['actual_cursor_description'] == RESULT_COLUMNS and metric['rows'] == ROWS[case]
                        and metric['diagnostic_enabled'] is diagnostic and metric['settings']['threads'] == 1
                        and metric['settings']['default_collation'] in ('', 'binary')
                        and metric['settings']['binary_behavior_probes'] == [False, False, False]
                        and metric['query_worker_does_not_read_source_rows_or_oracle'] is True,
                        'actual original SQL engine metadata/collation/worker boundary')
                if diagnostic:
                    require(bm.read_bytes() == expected_bitmap, 'complete actual engine selection bitmap')
                else:
                    require(not bm.exists(), 'primary engine no imposed diagnostic')
                answer = {'case': case, 'build': 'actual_duckdb', 'method': 'duckdb', 'diagnostic': diagnostic,
                    'result': compared, 'metrics_path': str(metrics), 'metrics': metric,
                    'bitmap_path': str(bm) if diagnostic else None, 'bitmap_sha256': sha(bm) if diagnostic else None,
                    'complete_bitmap_equal': diagnostic, 'no_timing_claim': True}
                answers.append(answer)
                comparisons.write(json.dumps(answer)+'\n')
        # Fresh explicitly malformed metadata copies: never alter any prepared fixture.
        malformed = out / 'malformed_not_null'
        malformed.mkdir()
        for field, column in [('state', 17), ('population', 13)]:
            folder = malformed / field
            folder.mkdir()
            source = prepared / 'MixedTail'
            names = ['binding.txt', 'state.dict', 'columns.tsv'] + [f'col{col}{suffix}'
                for col in [17, 13] for suffix in ['.u32', '.biased.u32', '.valid.bitmap']]
            for name in names:
                shutil.copyfile(source / name, folder / name)
            metadata = [line.split() for line in (folder / 'columns.tsv').read_text().splitlines()]
            for line in metadata:
                if int(line[0]) == column:
                    require(line[2] == '1', 'nullable fixture source before malformed NN change')
                    line[2] = '0'
            (folder / 'columns.tsv').write_text(''.join(' '.join(line)+'\n' for line in metadata))
            for path in folder.iterdir():
                bind(path, sha(path), input_ids)
            for build in ['normal', 'ubsan']:
                binary = out / f'adapter_{build}'
                for action in ['build', 'packed_query']:
                    name = f'reject_not_null_{field}_{build}_{action}'
                    destination = out / f'{name}.result'
                    if action == 'build':
                        command = [binary, 'build_fullsql', folder, destination]
                    else:
                        command = [binary, 'query_fullsql', out / f'MixedTail_{build}.bsql', folder,
                            ROOT / SQL_REL, 'packed_code_fused', destination, '-', out / f'{name}.metrics']
                    run(name, command, 15, expected_code=2)
                    require('NOT NULL' in (out / f'{name}.stderr').read_text() and
                            not destination.exists() and not (out / f'{name}.metrics').exists(),
                            'bounded malformed NOT NULL declaration must reject before output')
                    rejections.append({'build': build, 'case': f'MixedTail declared nonnullable {field}',
                                       'action': action, 'exit_code': 2})
        require(len(commands) == 235 and len(answers) == 182 and len(values) == 14 and
                len(helper_receipts) == 2 and len(rejections) == 10, 'fixed whole gate counts')
        require(sum(answer['diagnostic'] for answer in answers) == 91 and
                sum(answer['case'] == 'Observed' for answer in answers) == 26, 'fixed primary/diagnostic observed denominator')
        require(all(sha(Path(path)) == value for ids in [source_ids, input_ids, binary_ids] for path, value in ids.items()), 'final immutable identities')
        write_json(out / 'DISK_COMPLETION.json', {'free_bytes': shutil.disk_usage(out).free,
            'fresh_run_file_bytes_before_final_receipts': sum(p.stat().st_size for p in out.rglob('*') if p.is_file()),
            'projected_output_bound_bytes': PROJECTED_OUTPUT_BOUND_BYTES, 'shared_reserve_bytes': DISK_RESERVE_BYTES})
        write_json(out / 'READ_ONLY_INPUT_IDENTITIES.json', input_ids)
        require(len(representations) == 7, 'complete representation ledgers')
        write_json(out / 'REPRESENTATION_BYTES.json', representations)
        write_json(out / 'FULL_VALUE_VERIFICATIONS.json', values)
        write_json(out / 'OUTPUT_RECEIPTS.json', answers)
        write_json(out / 'summary.json', {'status': 'passed', 'scope': 'one complete original IG/2 SQL on pinned sample + six mathematical fixtures',
            'cases': CASES, 'methods': METHODS+['duckdb'], 'native_builds': ['normal', 'ubsan'],
            'commands': 235, 'complete_typed_results': 182, 'complete_diagnostic_bitmaps': 91,
            'native_full_value_validity_checks': 14, 'duckdb_full18_roundtrips': 7,
            'observed_complete_result_checks': 26, 'observed_primary_native_methods': 6,
            'helper_receipts': helper_receipts, 'preparer_helper_receipt': parser_helper,
            'bounded_rejection_receipts': rejections, 'native_rejection_commands': 10,
            'complete_original_query_coverage': {'passed': 1, 'roster': 8, 'new_query': 'IGlocations1/2.sql',
                'remaining_queries_not_claimed': ['Arade/2.sql', 'Food/1.sql', 'IGlocations1/1.sql', 'IGlocations1/16.sql',
                                                'IUBLibrary/1.sql', 'IUBLibrary/4.sql', 'IUBLibrary/5.sql']},
            'all_original_rows_preserved': True, 'full_dictionary_original_strings_restored': True,
            'all_complete_outputs_and_logs_retained': True, 'source_freeze_sha256': sha(ROOT / FREEZE),
            'no_general_sql_or_native_fls_claim': True, 'no_fresh_unseen_dataset_claim': True,
            'no_timing_claim': True, 'performance_selection_from_gate_forbidden': True,
            'representation_inventory': 'REPRESENTATION_BYTES.json',
            'representation_scope': 'native projected2 columns + literal dictionary vs actual full18-column database; no fulltable compression ratio'})
        status = 'passed'
    except BaseException as exc:
        error = f'{type(exc).__name__}: {exc}'
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        # All partial logs/artifacts survive failures; only this fresh run's files are created.
        write_json(out / 'PARTIAL_SOURCE_IDENTITIES.json', source_ids)
        write_json(out / 'PARTIAL_INPUT_IDENTITIES.json', input_ids)
        write_json(out / 'PARTIAL_OUTPUT_RECEIPTS.json', answers)
        write_json(out / 'PARTIAL_REPRESENTATION_BYTES.json', representations)
        terminal = {'status': status, 'error': error, 'start_utc': began, 'end_utc': utc(),
            'wall_s': time.monotonic()-start, 'cap_seconds': args.cap_seconds, 'commands': len(commands),
            'complete_typed_results': len(answers), 'complete_native_vector_checks': len(values),
            'completed_helper_workers': len(helper_receipts), 'completed_bounded_native_rejections': len(rejections),
            'children_reaped': all(cmd['reaped'] and cmd['worker_reaped_receipt'] for cmd in commands),
            'cpu_affinity': sorted(os.sched_getaffinity(0)), 'lock_released_after_receipt': True,
            'failures_and_partial_outputs_retained': True, 'automatic_retry': False, 'no_timing_claim': True}
        write_json(out / 'terminal.json', terminal)
        journal.close()
        comparisons.close()
        fcntl.flock(lock, fcntl.LOCK_UN)
        print(json.dumps(terminal))
    if status != 'passed':
        raise SystemExit(1)


if __name__ == '__main__':
    main()
