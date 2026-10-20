#!/bin/sh
# Container entry point: migrate (serialised by an advisory lock, so every
# replica can do it), make sure the worker's jobs exist, then become the
# requested command so it receives SIGTERM directly.
set -eu
if [ "${MV_MIGRATE_ON_START:-1}" = 1 ]; then
  macro-vintages migrate
fi
if [ "${1:-}" = worker ] && [ "${MV_SYNC_ON_START:-1}" = 1 ]; then
  macro-vintages sync-catalogue
fi
exec macro-vintages "$@"
