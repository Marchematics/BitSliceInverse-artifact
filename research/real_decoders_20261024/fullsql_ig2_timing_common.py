#!/usr/bin/env python3
"""Fixed IG/2 gate/timing controller helpers; copied safety pattern from its gate.

Not a worker and not a framework: all paths, limits, lineage and contracts below
are this single frozen seven-path experiment. Import never launches a process.
"""

import sys as _artifact_sys
from pathlib import Path as _ArtifactPath
_artifact_sys.path.insert(0, str(_ArtifactPath(__file__).resolve().parent))
import artifact_paths as _artifact
import fcntl
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import time
from run_fullsql_ig2_gate_v1r1 import (require, utc, sha, fingerprint, read_json,
    write_json, json_lines, finite_nonnegative, exact_result, RESULT_COLUMNS)

ROOT = Path(__file__).resolve().parent
WS = ROOT.parents[1]
SOURCE_GATE = ROOT / 'results/run_fullsql_ig2_gate_v1r1'
TIMING_GATE = ROOT / 'results/run_fullsql_ig2_timing_gate_v1'
FREEZE = 'FULL_SQL_IG2_TIMING_SOURCE_FREEZE.json'
CASES = ['Observed', 'MixedTail', 'AbsentLiteral', 'AllExcluded', 'AllNullPayload', 'Endpoints', 'Empty']
ROWS = dict(zip(CASES, [65536, 1031, 65, 63, 64, 1024, 0]))
NATIVE_METHODS = ['generic', 'expert', 'scan_fused', 'group_then_filter', 'cache_fused', 'packed_code_fused']
METHODS = NATIVE_METHODS + ['duckdb']
WORKER = ROOT / 'fullsql_ig2_timing_worker.py'
SQL_SHA = '0bc95092f92bbb2f8652e66c10db8bec5fba88b6d96c5ac23e9650fe27ecca8b'
DUCK_NATIVE = WS / 'bitslice_external_20261021/deps/site/_duckdb.abi3.so'
DUCK_SHA = 'e7cda112749d860a36cb1524ff465cefecdb0acc5c4e2ff4510ecc1b6f6c061a'
DISK_RESERVE = 512*1024*1024


class Stage:
    def __init__(self, run_id, cap, phase, output_bound):
        require(run_id.isidentifier(), 'fresh identifier run id')
        self.lock = (WS / 'benchmark_cpu.lock').open('a+')
        fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        self.out = ROOT / 'results' / run_id
        try:
            self.out.mkdir(exist_ok=False)
        except BaseException:
            fcntl.flock(self.lock, fcntl.LOCK_UN)
            self.lock.close()
            raise
        self.start, self.began = time.monotonic(), utc()
        self.deadline, self.cap, self.phase = self.start+cap, cap, phase
        self.commands, self.outputs = [], []
        self.sources, self.inputs, self.binaries, self.stats = {}, {}, {}, {}
        self.journal = (self.out / 'journal.jsonl').open('x', buffering=1)
        self.observations = (self.out / ('qualification_outputs.jsonl' if phase == 'gate' else 'timing_observations.jsonl')).open('x', buffering=1)
        self.env = os.environ.copy()
        self.env['PYTHONDONTWRITEBYTECODE'] = '1'
        self.env.update({key: '1' for key in ['OMP_NUM_THREADS', 'OPENBLAS_NUM_THREADS', 'MKL_NUM_THREADS',
            'NUMEXPR_NUM_THREADS', 'RAYON_NUM_THREADS', 'TOKIO_WORKER_THREADS']})
        self.output_bound = output_bound
        def expire(signum, frame):
            raise TimeoutError('full SQL timing '+phase+' whole stage cap expired')
        signal.signal(signal.SIGALRM, expire)
        signal.setitimer(signal.ITIMER_REAL, max(.001, self.deadline-time.monotonic()))

    def bind(self, path, expected, target):
        path = Path(path).resolve()
        key = str(path)
        require(key not in target or target[key] == expected, 'conflicting frozen identity '+key)
        require(sha(path) == expected, 'frozen identity '+key)
        target[key], self.stats[key] = expected, fingerprint(path)

    def run(self, name, command, cap=30):
        require(all(fingerprint(Path(path)) == stat for path, stat in self.stats.items()), 'source/input/binary stat changed')
        require(shutil.disk_usage(self.out).free >= DISK_RESERVE, 'shared disk reserve before worker')
        remaining = self.deadline-time.monotonic()
        require(remaining > 0, 'stage deadline')
        tick, begin = time.perf_counter(), utc()
        process, code, interrupted = None, None, None
        with (self.out / (name+'.stdout')).open('xb') as so, (self.out / (name+'.stderr')).open('xb') as se:
            try:
                previous = signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGALRM})
                try:
                    process = subprocess.Popen([sys.executable, str(ROOT / 'process_receipt.py'),
                        str(self.out / (name+'.resource.json')), *map(str, command)], cwd=ROOT,
                        env=self.env, stdout=so, stderr=se, start_new_session=True,
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
        rp = self.out / (name+'.resource.json')
        record = {'name': name, 'command': list(map(str, command)), 'start_utc': begin, 'end_utc': utc(),
            'wall_s': time.perf_counter()-tick, 'exit_code': code, 'pid': process.pid if process else None,
            'timed_out': isinstance(interrupted, (TimeoutError, subprocess.TimeoutExpired)),
            'interrupted': interrupted is not None, 'reaped': process is not None and process.returncode is not None,
            'worker_reaped_receipt': rp.exists()}
        self.commands.append(record)
        self.journal.write(json.dumps(record)+'\n')
        self.journal.flush()
        os.fsync(self.journal.fileno())
        if interrupted is not None:
            raise interrupted
        require(code == 0 and rp.exists(), name+' worker failed or resource receipt absent')
        require(shutil.disk_usage(self.out).free >= DISK_RESERVE, 'shared disk reserve crossed; retain partial evidence')
        resource = read_json(rp)
        require(resource['exit_code'] == 0 and finite_nonnegative(resource['wall_s']) and
                type(resource['max_rss_kib']) is int and resource['max_rss_kib'] > 0, name+' complete resource receipt')
        return resource

    def append(self, record):
        self.outputs.append(record)
        self.observations.write(json.dumps(record, ensure_ascii=False, allow_nan=False)+'\n')
        self.observations.flush()

    def prepare(self):
        os.sched_setaffinity(0, {min(os.sched_getaffinity(0))})
        cpu, proc_error, proc = min(os.sched_getaffinity(0)), None, ''
        try:
            proc = Path('/proc/cpuinfo').read_text()
        except OSError as exc:
            proc_error = str(exc)
        records = [{k.strip(): v.strip() for k,v in (line.split(':',1) for line in block.splitlines() if ':' in line)}
                   for block in proc.split('\n\n') if block.strip()]
        selected = next((row for row in records if row.get('processor') == str(cpu)), {})
        flags = selected.get('flags', '').split()
        available = 'sse2' in flags if flags else os.uname().machine.lower() in ('x86_64', 'amd64')
        write_json(self.out / 'CPU_IDENTITY.json', {'uname': list(os.uname()), 'affinity': [cpu],
            'proc_record': selected, 'proc_error': proc_error, 'sse2_precheck': available,
            'native_runtime_feature_check_retained': True})
        require(available, 'SSE2 unavailable; no substitute')
        write_json(self.out / 'DISK_PREFLIGHT.json', {'free_bytes': shutil.disk_usage(self.out).free,
            'shared_reserve_bytes': DISK_RESERVE, 'prospective_output_bytes': self.output_bound})
        require(shutil.disk_usage(self.out).free >= DISK_RESERVE+self.output_bound, 'shared reserve + prospective output headroom')
        frozen = read_json(ROOT / FREEZE)
        required = {'fullsql_ig2_timing_common.py', 'fullsql_ig2_timing_worker.py',
            'run_fullsql_ig2_timing_gate.py', 'run_fullsql_ig2_timing.py',
            'src_fullsql_ig2_timing/adapter.cpp', 'src_fullsql_ig2_timing/qualified_reference.hpp',
            'src_fullsql_ig2_timing/DERIVATION_RECEIPT.json', 'PROTOCOL_FULL_SQL_IG2_TIMING.md',
            'FULL_SQL_IG2_TIMING_ORDERS.json', 'run_fullsql_ig2_gate_v1r1.py', 'process_receipt.py',
            *[str((SOURCE_GATE / name).relative_to(ROOT)) for name in ['terminal.json', 'summary.json',
                'SOURCE_IDENTITIES.json', 'READ_ONLY_INPUT_IDENTITIES.json', 'BINARY_IDENTITIES.json',
                'REPRESENTATION_BYTES.json', 'FULL_SQL_IG2_V1R1_SOURCE_FREEZE.json']]}
        require(required <= set(frozen['files']), 'new timing freeze missing exact source/qualification')
        for relative, expected in frozen['files'].items():
            path = ROOT / relative
            require(path.resolve().is_relative_to(ROOT.resolve()), 'unsafe source freeze path')
            self.bind(path, expected, self.sources)
        self.bind(ROOT / FREEZE, sha(ROOT / FREEZE), self.sources)
        for path, expected in read_json(SOURCE_GATE / 'SOURCE_IDENTITIES.json').items():
            self.bind(path, expected, self.sources)
        require((ROOT / 'FULL_SQL_IG2_V1R1_SOURCE_FREEZE.json').read_bytes() ==
                (SOURCE_GATE / 'FULL_SQL_IG2_V1R1_SOURCE_FREEZE.json').read_bytes(), 'original qualification source freeze')
        passed, terminal = read_json(SOURCE_GATE / 'summary.json'), read_json(SOURCE_GATE / 'terminal.json')
        require(passed['status'] == terminal['status'] == 'passed' and passed['commands'] == 235 and
                passed['complete_typed_results'] == 182 and passed['complete_diagnostic_bitmaps'] == 91 and
                passed['native_full_value_validity_checks'] == 14 and passed['duckdb_full18_roundtrips'] == 7,
                'fixed full original SQL qualification incomplete')
        for manifest in ['READ_ONLY_INPUT_IDENTITIES.json', 'BINARY_IDENTITIES.json']:
            for path, expected in read_json(SOURCE_GATE / manifest).items():
                self.bind(path, expected, self.inputs)
        self.bind(DUCK_NATIVE, DUCK_SHA, self.inputs)
        source = (ROOT / 'src_fullsql_ig2/adapter.cpp').read_bytes()
        reference = (ROOT / 'src_fullsql_ig2_timing/qualified_reference.hpp').read_bytes()
        old = b'int main(int argc,char**argv){try{return sql_entry(argc,argv);}'
        new = b'static int fullsql_timing_inner_entry(int argc,char**argv){try{return sql_entry(argc,argv);}'
        require(source.count(old) == reference.count(new) == 1 and reference.replace(new,old,1) == source,
                'timing reference differs beyond sole entrypoint name')
        derivation = read_json(ROOT / 'src_fullsql_ig2_timing/DERIVATION_RECEIPT.json')
        require(derivation['source_sha256'] == sha(ROOT / 'src_fullsql_ig2/adapter.cpp') and
                derivation['derived_reference_sha256'] == sha(ROOT / 'src_fullsql_ig2_timing/qualified_reference.hpp') and
                derivation['kernel_frontend_aggregate_serializer_changed'] is False, 'exact timing derivation receipt')
        require(all(str(path.resolve()) in self.sources for path in (ROOT / 'src_fullsql_ig2_timing').iterdir() if path.is_file()),
                'unfrozen timing wrapper input')
        for case in CASES:
            folder = SOURCE_GATE / 'prepared' / case
            require(sha(folder / 'query.sql') == SQL_SHA, 'original query file')
            for path in [folder / 'oracle.json', folder / 'case.json', folder / 'query.sql',
                         SOURCE_GATE / f'duckdb_{case}/full18.duckdb']:
                require(str(path.resolve()) in self.inputs, 'qualified input absent from manifest')
        shutil.copyfile(ROOT / FREEZE, self.out / FREEZE)
        write_json(self.out / 'SOURCE_IDENTITIES.json', self.sources)
        write_json(self.out / 'READ_ONLY_INPUT_IDENTITIES.json', self.inputs)
        write_json(self.out / 'LINEAGE.json', {'source_gate': str(SOURCE_GATE),
            'source_gate_summary_sha256': sha(SOURCE_GATE / 'summary.json'),
            'source_freeze_sha256': sha(ROOT / FREEZE), 'prior_gate_timings_used_for_selection': False,
            'six_native_paths_same_wrapper_binary_per_build': True,
            'stored_input_interval': 'runtime-ready through original entry return/output close/local destruction',
            'process_wall_scope': 'fresh fork/exec/wait4 child lifecycle',
            'cold_disk_claim': False, 'warm_resident_state_claim': False,
            'OS_file_cache': 'not flushed; warm/unspecified filesystem cache',
            'representation_inventory': str(SOURCE_GATE / 'REPRESENTATION_BYTES.json'),
            'preprocessing_costs_excluded_from_query_but_separately_retained': True,
            'no_competitive_build_or_cross_projection_compression_claim': True})

    def audit(self):
        output = self.out / 'DUCKDB_WORKER_AUDIT.json'
        resource = self.run('duckdb_worker_audit', [sys.executable, WORKER, 'audit',
            SOURCE_GATE / 'duckdb_Observed/full18.duckdb', output], 30)
        audit = read_json(output)
        require(audit['status'] == 'passed' and audit['not_a_query_timing_observation'] is True and
                audit['identity']['version'] == '2.0.0' and audit['settings']['threads'] == 1 and
                audit['settings']['binary_behavior_probes'] == [False, False, False] and
                audit['settings']['default_collation'] in ('', 'binary'), 'actual timing-worker audit')
        self.bind(Path(audit['identity']['python_module']), sha(Path(audit['identity']['python_module'])), self.inputs)
        require(Path(audit['identity']['native_module']).resolve() == DUCK_NATIVE.resolve(), 'actual module path audit')
        self.api_identity = audit['identity']
        return resource

    def query(self, case, method, binary, name):
        folder = SOURCE_GATE / 'prepared' / case
        result, metrics = self.out / (name+'.result.json'), self.out / (name+'.metrics.jsonl')
        if method == 'duckdb':
            command = [sys.executable, WORKER, 'query', SOURCE_GATE / f'duckdb_{case}/full18.duckdb',
                       folder / 'query.sql', result, metrics]
        else:
            command = [binary, 'query_fullsql', SOURCE_GATE / f'{case}_normal.bsql', folder,
                       folder / 'query.sql', method, result, '-', metrics]
        resource = self.run(name, command)
        # Independent full verification is outside both measured worker intervals.
        compared = exact_result(result, read_json(folder / 'oracle.json'))
        rows = json_lines(metrics)
        require(len(rows) == 2, 'exact inner and outer operational receipts')
        phase, outer = rows
        require(outer['kind'] == 'stored_input_timing' and outer['method'] == method and
                finite_nonnegative(outer['stored_input_e2e_ms']) and outer['stored_input_e2e_ms'] > 0,
                'complete stored-input interval')
        for key in ['runtime_ready_before_input_open', 'original_query_entry_returned',
                    'complete_result_file_closed', 'query_locals_destroyed', 'original_inner_metrics_included']:
            require(outer[key] is True, 'stored interval boundary '+key)
        require(outer['outer_receipt_included'] is False and phase['method'] == method and
                phase['diagnostic_enabled'] is False and phase['result_bytes'] == result.stat().st_size,
                'complete primary output receipt')
        require(outer['stored_input_e2e_ms'] <= resource['wall_s']*1000+1.0,
                'stored interval exceeds whole worker beyond1ms clock/serialization tolerance')
        if method == 'duckdb':
            require(outer['identity'] == self.api_identity and phase['actual_cursor_description'] == RESULT_COLUMNS and
                    phase['full_result_materialized'] is True and phase['original_sql_executions'] == 1 and
                    phase['source_rows_or_oracle_read'] is False and phase['binary_or_database_hashing'] is False and
                    phase['metadata_or_behavior_probe_queries'] is False and phase['requested_threads'] == 1,
                    'actual timing-only DuckDB contract')
        else:
            require(phase['rows'] == ROWS[case] and phase['sum_type'] == 'HUGEINT' and phase['runtime_sse2'] is True and
                    phase['full_result_serialization_charged'] is True and
                    phase['group_key_filter_after_aggregate'] is (method == 'group_then_filter') and
                    phase['selection_bitmap_bytes'] == (8*((ROWS[case]+63)//64) if method in ['generic','expert'] else 0)
                    and phase['diagnostic_bitmap_bytes'] == 0, 'same native primary contract')
        return {'case': case, 'method': method, 'stored_input_e2e_ms': outer['stored_input_e2e_ms'],
            'resource': resource, 'result_path': str(result), 'result_sha256': compared['sha256'],
            'result_bytes': result.stat().st_size, 'complete_typed_result_equal': True,
            'phase_metrics_path': str(metrics), 'phase_metrics_sha256': sha(metrics),
            'phase_metrics': phase, 'outer_receipt': outer,
            'complete_result_rows': compared['full_rows_compared']}

    def final_identities(self):
        require(all(sha(Path(path)) == expected for ids in [self.sources,self.inputs,self.binaries]
                    for path,expected in ids.items()), 'final immutable source/input/binary identity')
        write_json(self.out / 'FINAL_INPUT_IDENTITIES.json', self.inputs)
        write_json(self.out / 'BINARY_IDENTITIES.json', self.binaries)

    def finish(self, status, error, summary):
        signal.setitimer(signal.ITIMER_REAL, 0)
        if status == 'passed':
            write_json(self.out / 'summary.json', summary)
        write_json(self.out / 'PARTIAL_OUTPUT_RECEIPTS.json', self.outputs)
        write_json(self.out / 'PARTIAL_INPUT_IDENTITIES.json', self.inputs)
        terminal = {'status': status, 'error': error, 'phase': self.phase, 'start_utc': self.began,
            'end_utc': utc(), 'wall_s': time.monotonic()-self.start, 'cap_seconds': self.cap,
            'commands': len(self.commands), 'complete_typed_outputs': len(self.outputs),
            'children_reaped': all(row['reaped'] and row['worker_reaped_receipt'] for row in self.commands),
            'source_freeze_sha256': sha(ROOT / FREEZE) if (ROOT / FREEZE).exists() else None,
            'cpu_affinity': sorted(os.sched_getaffinity(0)), 'automatic_retry': False,
            'partial_and_failed_evidence_retained': True, 'lock_released_after_receipt': True}
        write_json(self.out / 'terminal.json', terminal)
        self.journal.close()
        self.observations.close()
        fcntl.flock(self.lock, fcntl.LOCK_UN)
        self.lock.close()
        print(json.dumps(terminal))
