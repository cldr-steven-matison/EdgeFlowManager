#!/bin/bash
# Usage: agent-startup.sh
#
# Minimum-tier startup after a host/WSL2 restart: resumes minikube (does NOT
# delete/recreate), then restores the two headless port-forwards everything
# else depends on:
#   - vllm-service:8000  (ClusterIP, no tunnel alternative — this is OpenClaw's
#                          own LLM brain; without it the Telegram bot can't think)
#   - cso-operator-app:8090 (LoadBalancer — plain port-forward instead of
#                          `minikube tunnel` so this works headless/remote,
#                          no sudo, no browser)
#
# Idempotent: safe to re-run, skips anything already answering. Does NOT run
# `minikube tunnel` — that's a separate manual step for local NiFi/Surveyor/EFM
# UI browsing (run it yourself at the desk: `minikube tunnel`). If you do run
# tunnel afterward, it will conflict with this script's cso-operator-app
# forward on port 8090 specifically (both bind 127.0.0.1:8090) — kill this
# script's forward first: `kill $(cat ~/.cache/agent-startup/cso-operator-app.pid)`

set -e

APP_URL="${APP_URL:-http://127.0.0.1:8090}"
VLLM_URL="${VLLM_URL:-http://127.0.0.1:8000}"
LOG_DIR="${LOG_DIR:-$HOME/.cache/agent-startup}"
mkdir -p "$LOG_DIR"

notify() {
    echo "$1"
    if [ -n "$TOKEN" ] && [ -n "$CHAT_ID" ]; then
        curl -s -X POST "https://api.telegram.org/bot$TOKEN/sendMessage" \
             -d "chat_id=$CHAT_ID" -d "text=$1" > /dev/null
    fi
}

# 1. Minikube itself — resume, never delete
if minikube status > /dev/null 2>&1; then
    echo "✅ minikube already running"
else
    echo "🚀 minikube not running — starting (resume, not delete)..."
    minikube start
fi

# 2. Wait for the two deployments that matter to report Ready
echo "⏳ waiting for vllm-server and cso-operator-app rollouts..."
kubectl rollout status deployment/vllm-server -n default --timeout=180s
kubectl rollout status deployment/cso-operator-app -n default --timeout=180s

# 3. Port-forwards — idempotent, skip if already answering
ensure_forward() {
    local label="$1" svc="$2" ns="$3" local_port="$4" svc_port="$5" health_url="$6"
    local pidfile="$LOG_DIR/${label}.pid"

    if curl -s -o /dev/null -m 2 "$health_url"; then
        echo "✅ $label already reachable at $health_url"
        return 0
    fi

    if [ -f "$pidfile" ] && kill -0 "$(cat "$pidfile")" 2>/dev/null; then
        kill "$(cat "$pidfile")" 2>/dev/null || true
    fi

    echo "🚀 starting port-forward for $label ($local_port -> svc/$svc:$svc_port)..."
    nohup kubectl port-forward -n "$ns" "svc/$svc" "${local_port}:${svc_port}" \
        > "$LOG_DIR/${label}.log" 2>&1 &
    echo $! > "$pidfile"

    for _ in $(seq 1 15); do
        curl -s -o /dev/null -m 2 "$health_url" && { echo "✅ $label is up"; return 0; }
        sleep 1
    done

    echo "❌ $label port-forward did not come up — check $LOG_DIR/${label}.log"
    return 1
}

FAILED=0
ensure_forward "vllm" "vllm-service" "default" "8000" "8000" "$VLLM_URL/v1/models" || FAILED=1
ensure_forward "cso-operator-app" "cso-operator-app" "default" "8090" "8090" "$APP_URL/api/health" || FAILED=1

# 4. Roll up backing-service health via the app's own /api/health (nifi, kafka,
#    qdrant, embedding, whisper) instead of hand-waiting on every deployment
if [ "$FAILED" -eq 0 ]; then
    HEALTH=$(curl -s -m 5 "$APP_URL/api/health")
    BAD=$(echo "$HEALTH" | jq -r '.services | to_entries[] | select(.value.ok != true) | .key' 2>/dev/null)
    if [ -n "$BAD" ]; then
        notify "⚠️ Minimum services online (vllm:8000, app:8090) but backing services not-ok: $BAD"
    else
        notify "✅ Minikube resumed, minimum services online — vllm:8000 and app:8090 reachable, all backing services ok. (minikube tunnel NOT started — run it yourself for local NiFi/Surveyor/EFM UI)"
    fi
else
    notify "❌ agent-startup.sh: one or more port-forwards failed to come up — check $LOG_DIR logs"
    exit 1
fi
