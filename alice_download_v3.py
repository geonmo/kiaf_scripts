#!/usr/bin/env python3
import os
import sys
import subprocess
import pickle
from multiprocessing import Pool
from optparse import OptionParser

if os.environ.get("ALICE_ENV") != "1":
    print("ALICE_ENV 환경변수가 1로 설정되어 있지 않습니다.")
    print("kiafenv 스크립트를 실행한 후 다시 시도하세요.")
    sys.exit(1)

DESTINATION_BASE = "file:///data/xrdnamespace/"
MAX_PROCESSES = 20
verbose = False

def log(message):
    if verbose:
        print(message)

def load_size_cache(filelist_path):
    cache_file = f"alien.sizecache.pkl"
    if os.path.exists(cache_file):
        try:
            with open(cache_file, 'rb') as f:
                cache = pickle.load(f)
            log(f"Loaded size cache from {cache_file}")
            return cache
        except Exception as e:
            log(f"Failed to load cache from {cache_file}: {e}")
            return {}
    return {}

def save_size_cache(filelist_path, cache):
    cache_file = f"alien.sizecache.pkl"
    try:
        with open(cache_file, 'wb') as f:
            pickle.dump(cache, f)
        log(f"Saved size cache to {cache_file}")
    except Exception as e:
        log(f"Failed to save cache to {cache_file}: {e}")

def get_file_size_alien_stat(src):
    log(f"Entering get_file_size_alien_stat with src={src}")
    try:
        result = subprocess.run(['alien_stat', src], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        for line in result.stdout.splitlines():
            if line.startswith('Size:'):
                size = int(line.split()[1])
                log(f"Exiting get_file_size_alien_stat with size={size}")
                return size
    except Exception as e:
        log(f"Exception in get_file_size_alien_stat: {e}")
        return None
    log(f"Exiting get_file_size_alien_stat with None for {src}")
    return None

def get_file_size_stat(dest):
    log(f"Entering get_file_size_stat with dest={dest}")
    try:
        result = subprocess.run(['stat', dest], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        for line in result.stdout.splitlines():
            if line.strip().startswith('Size:'):
                size = int(line.strip().split()[1])
                log(f"Exiting get_file_size_stat with size={size}")
                return size
    except Exception as e:
        log(f"Exception in get_file_size_stat: {e}")
        return None
    log("Exiting get_file_size_stat with None")
    return None

def run_alien_cp(args):
    idx, total, src, dryrun, src_size_dict = args
    prefix = f"[{idx+1}/{total}]"
    log(f"Entering run_alien_cp for {prefix} src={src}")

    dest = f"{DESTINATION_BASE}{src}"
    cmd = ["alien_cp", " -timeout","1800"," -f", src.strip(), dest]

    src_size = src_size_dict.get(src, None)
    dest_path = dest.replace("file://", "")
    dest_size = get_file_size_stat(dest_path)

    if src_size is not None and dest_size is not None and src_size == dest_size:
        log(f"File already exists with same size for {src}, skipping transfer.")
        log(f"Exiting run_alien_cp for {prefix} src={src} with skip")
        return (prefix, src, True, "File already exists with same size, skipping transfer.")

    if dryrun:
        log(f"Dryrun mode: would run {' '.join(cmd)}")
        log(f"Exiting run_alien_cp for {prefix} src={src} with dryrun")
        return (prefix, src, True, f"DRYRUN: {' '.join(cmd)}")
    try:
        print(f"Start to download {src}")
        result = subprocess.run(
            cmd,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        print(f"Successfully ran alien_cp for {src}")
        log(f"Exiting run_alien_cp for {prefix} src={src} with success")
        return (prefix, src, True, result.stdout)
    except subprocess.CalledProcessError as e:
        print(f"alien_cp failed for {src} with error: {e.stderr}")
        log(f"Exiting run_alien_cp for {prefix} src={src} with failure")
        return (prefix, src, False, e.stderr)
    except Exception as e:
        print(f"Exception in run_alien_cp for {src}: {e}")
        log(f"Exiting run_alien_cp for {prefix} src={src} with exception")
        return (prefix, src, False, str(e))

def main():
    global verbose
    parser = OptionParser(usage="usage: %prog [options] FILELIST")
    parser.add_option(
        "-n", "--dryrun",
        action="store_true",
        dest="dryrun",
        default=False,
        help="실제로 전송하지 않고 실행할 명령만 출력합니다."
    )
    parser.add_option(
        "-l", "--limit",
        dest="limit",
        type="int",
        default=None,
        help="처리할 파일 개수 제한 (예: -l 5)"
    )
    parser.add_option(
        "-v", "--verbose",
        action="store_true",
        dest="verbose",
        default=False,
        help="상세 로그를 출력합니다."
    )
    (options, args) = parser.parse_args()

    if len(args) < 1:
        parser.error("FILELIST 파일명을 지정해야 합니다.")

    filelist_path = args[0]
    dryrun = options.dryrun
    limit = options.limit
    verbose = options.verbose

    log(f"main() started with filelist_path={filelist_path}, dryrun={dryrun}, limit={limit}, verbose={verbose}")

    if not os.path.exists(filelist_path):
        print(f"Error: File {filelist_path} not found!")
        return

    try:
        with open(filelist_path, "r") as f:
            src_list = [line.strip() for line in f if line.strip()]
    except IOError as e:
        print(f"File read error: {str(e)}")
        return

    if limit is not None and limit > 0:
        src_list = src_list[:limit]

    total = len(src_list)
    size_cache = load_size_cache(filelist_path)

    # 캐시를 미리 채움
    updated = False
    for src in src_list:
        if src not in size_cache:
            size = get_file_size_alien_stat(src)
            if size is not None:
                size_cache[src] = size
                updated = True
            else:
                print(f"Warning: Could not get size for {src}, skipping this file.")
    if updated:
        save_size_cache(filelist_path, size_cache)

    # 워커에는 캐시 dict를 전달 (읽기만 함)
    task_list = [(i, total, src, dryrun, size_cache) for i, src in enumerate(src_list)]

    log(f"Starting multiprocessing pool with {MAX_PROCESSES} processes and {total} tasks")
    with Pool(processes=MAX_PROCESSES) as pool:
        results = pool.map(run_alien_cp, task_list)

        success_count = 0
        for prefix, src, success, output in results:
            if dryrun:
                print(f"{prefix} {output}")
                success_count += 1
            elif success:
                print(f"{prefix} ✅ Success: {src}")
                success_count += 1
            else:
                print(f"{prefix} ❌ Failed: {src}")
                print(f"   Error: {output}")

        print(f"\nTotal: {total}, Success: {success_count}, Failed: {total-success_count}")

    log("Finished multiprocessing pool")

if __name__ == "__main__":
    main()

