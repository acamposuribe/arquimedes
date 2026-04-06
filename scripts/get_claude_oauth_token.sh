#!/bin/sh
set -eu

security find-generic-password -s "Claude Code-credentials" -w \
  | /usr/bin/python3 -c 'import json,sys; print(json.loads(sys.stdin.read())["claudeAiOauth"]["accessToken"])'
