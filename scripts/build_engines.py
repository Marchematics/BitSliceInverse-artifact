#!/usr/bin/env python3
"""Build the DuckDB table function against the installed wheel's C API."""
import os
from pathlib import Path
import platform
import shlex
import subprocess
import sys
import sysconfig
import urllib.request
import zipfile

import _duckdb
import duckdb

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT/'research/real_decoders_20261024/src_crossengine_20261101'
OUT = ROOT/'work/crossengine/build'
INC = ROOT/'work/deps/duckdb'


def main():
    OUT.mkdir(parents=True,exist_ok=True)
    INC.mkdir(parents=True,exist_ok=True)
    header=INC/'duckdb.h'
    version=duckdb.__version__.split('.dev')[0]
    if not header.exists():
        header.write_bytes(urllib.request.urlopen(
            f'https://raw.githubusercontent.com/duckdb/duckdb/v{version}/src/include/duckdb.h',timeout=60).read())
    flags=['-std=c++20','-O3','-DNDEBUG','-I',str(INC),'-I',str(SRC)]
    if platform.machine().lower() in {'amd64','x86_64'}:flags+=['-march=native']
    libdir=Path(sysconfig.get_config_var('LIBDIR'))
    libname=sysconfig.get_config_var('LDLIBRARY')
    library=libdir/libname
    if not library.exists():
        libdir=Path(sys.executable).resolve().parents[1]/'lib'
        library=libdir/libname
    links=[str(Path(_duckdb.__file__).resolve())]
    if library.exists():links.append(str(library))
    if platform.system()=='Darwin':
        archive=INC/'libduckdb.zip'
        if not archive.exists():
            archive.write_bytes(urllib.request.urlopen(
                f'https://github.com/duckdb/duckdb/releases/download/v{version}/libduckdb-osx-universal.zip',timeout=120).read())
        with zipfile.ZipFile(archive) as z:
            for name in z.namelist():
                if name.endswith('libduckdb.dylib'):
                    (INC/'libduckdb.dylib').write_bytes(z.read(name))
        links=[str(INC/'libduckdb.dylib'),'-Wl,-rpath,'+str(INC)]
    else:
        links+=['-Wl,-rpath,'+str(Path(_duckdb.__file__).parent)+':'+str(libdir)]
    subprocess.run(shlex.split(os.environ.get('CXX','c++'))+flags+
                   [str(SRC/'ecube_duck.cpp'),'-o',str(OUT/'ecube_duck')]+links,check=True)
    print(f'built ecube_duck with DuckDB {duckdb.__version__}')


if __name__=='__main__':main()
