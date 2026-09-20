#!/bin/zsh
set -euo pipefail

export PATH="/opt/homebrew/bin:/opt/homebrew/sbin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"

BIN="/Users/codexify/.codexify/bin/codexify"
NEXT="/Users/codexify/.codexify/bin/codexify.next"
BACKUP="/Users/codexify/.codexify/bin/codexify.pre-auto-continue"
CONFIG="/Users/codexify/.codexify/codexify.config.json"

wait_for_health() {
  local code
  for _ in {1..30}; do
    code=$(/usr/bin/curl -sS -o /dev/null -w '%{http_code}' --max-time 1 http://127.0.0.1:3000/ 2>/dev/null || true)
    [[ "$code" == "401" ]] && return 0
    /bin/sleep 1
  done
  return 1
}

swap_binary() {
  local source="$1"
  local temporary="${BIN}.swap.$$"
  /bin/cp -p "$source" "$temporary"
  /bin/chmod 755 "$temporary"
  /bin/mv -f "$temporary" "$BIN"
}

service_pid() {
  /usr/bin/pgrep -u "$(/usr/bin/id -u)" -f "^${BIN} service run --config ${CONFIG}$" | /usr/bin/head -1 || true
}

restart_service() {
  local old_pid new_pid
  old_pid="$(service_pid)"
  if [[ -n "$old_pid" ]]; then
    /bin/kill -TERM "$old_pid"
  fi
  for _ in {1..30}; do
    new_pid="$(service_pid)"
    if [[ -n "$new_pid" && ( -z "$old_pid" || "$new_pid" != "$old_pid" ) ]]; then
      /bin/sleep 1
      /bin/kill -0 "$new_pid" 2>/dev/null && return 0
    fi
    /bin/sleep 1
  done
  echo "Codexify service did not become healthy under launchd KeepAlive." >&2
  return 1
}

rollback() {
  [[ -x "$BACKUP" ]] || { echo "Backup not found: $BACKUP" >&2; return 1; }
  echo "Restoring $BACKUP"
  swap_binary "$BACKUP"
  restart_service
  wait_for_health
  echo "Rollback healthy."
}

if [[ "${1:-}" == "rollback" ]]; then
  rollback
  exit 0
fi

[[ -x "$BIN" ]] || { echo "Current Codexify binary not found: $BIN" >&2; exit 1; }
[[ -x "$NEXT" ]] || { echo "Experimental binary not found: $NEXT" >&2; exit 1; }
"$NEXT" --help >/dev/null
/usr/bin/file "$NEXT" | /usr/bin/grep -q 'Mach-O 64-bit executable arm64' || {
  echo "Experimental binary is not macOS arm64: $NEXT" >&2
  exit 1
}

if [[ ! -e "$BACKUP" ]]; then
  /bin/cp -p "$BIN" "$BACKUP"
  /bin/chmod 755 "$BACKUP"
  echo "Saved rollback binary: $BACKUP"
else
  echo "Keeping existing rollback binary: $BACKUP"
fi

echo "Installing experimental binary; launchd KeepAlive will restart the service"
swap_binary "$NEXT"
if ! restart_service || ! wait_for_health; then
  echo "Experimental Codexify failed its localhost health check; rolling back." >&2
  rollback || echo "Automatic rollback failed; run this script with 'rollback'." >&2
  exit 1
fi

echo "Experimental Codexify is healthy on 127.0.0.1:3000."
echo "Manual rollback: $0 rollback"
