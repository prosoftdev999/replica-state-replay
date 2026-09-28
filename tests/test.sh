#!/bin/sh
set -u
mkdir -p /logs/verifier
printf '0\n' > /logs/verifier/reward.txt
cleanup() {
  code=$?
  if [ ! -s /logs/verifier/ctrf.json ]; then
    printf '{"results":{"tool":{"name":"pytest"},"summary":{"tests":1,"passed":0,"failed":1,"pending":0,"skipped":0,"other":0,"suites":1},"tests":[]}}\n' > /logs/verifier/ctrf.json
  fi
  exit "$code"
}
trap cleanup EXIT INT TERM
if pytest -q /tests/test_replay.py --ctrf=/logs/verifier/ctrf.json; then
  printf '1\n' > /logs/verifier/reward.txt
  exit 0
else
  printf '0\n' > /logs/verifier/reward.txt
  exit 1
fi
