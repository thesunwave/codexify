#!/bin/zsh
set -euo pipefail

export PATH="/opt/homebrew/bin:/opt/homebrew/sbin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"

BIN="/Users/codexify/.codexify/bin/codexify"
NEXT="/Users/codexify/.codexify/bin/codexify.next"
BACKUP="/Users/codexify/.codexify/bin/codexify.pre-auto-continue"
LABEL="system/dev.codexify.service"

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

restart_service() {
  /usr/bin/sudo /bin/launchctl kickstart -k "$LABEL"
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
  /usr/bin/sudo -v
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

/usr/bin/sudo -v
if [[ ! -e "$BACKUP" ]]; then
  /bin/cp -p "$BIN" "$BACKUP"
  /bin/chmod 755 "$BACKUP"
  echo "Saved rollback binary: $BACKUP"
else
  echo "Keeping existing rollback binary: $BACKUP"
fi

echo "Installing experimental binary and restarting $LABEL"
swap_binary "$NEXT"
if ! restart_service || ! wait_for_health; then
  echo "Experimental Codexify failed its localhost health check; rolling back." >&2
  rollback || echo "Automatic rollback failed; run this script with 'rollback'." >&2
  exit 1
fi

echo "Experimental Codexify is healthy on 127.0.0.1:3000."
echo "Manual rollback: $0 rollback"
