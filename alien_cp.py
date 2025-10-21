#!/usr/bin/env python3
import os, subprocess, shlex, sys

def load_env_from_script(script_path: str) -> dict:
    # env -0: 널 문자 구분으로 안전하게 파싱 (값에 개행/공백 포함 가능)
    # source 과정에서 출력되는 메시지는 /dev/null 로 버립니다.
    bash_cmd = f'set -a; source {shlex.quote(script_path)} >/dev/null 2>&1; env -0'
    proc = subprocess.run(
        ["/bin/bash", "-c", bash_cmd],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=True,
    )
    env = {}
    for entry in proc.stdout.split(b"\0"):
        if not entry:
            continue
        k, _, v = entry.partition(b"=")
        env[k.decode("utf-8", "ignore")] = v.decode("utf-8", "ignore")
    return env

# 1) /pool/kiafenv 를 source해서 환경 받아오기
env_after = load_env_from_script("/pool/kiafenv")

# 2) 그 환경을 현재 Python에도 반영(선택)
os.environ.update(env_after)

# 3) 이제 이 환경으로 원하는 명령을 몇 번이든 실행

cmd = "alien_cp"+ ' '+' '.join(shlex.quote(a) for a in sys.argv[1:])
subprocess.run(
    ["/bin/bash", "-c", cmd],
    check=True,
    env=env_after,   # 또는 생략하고 os.environ.update 했으면 생략 가능
)


