#!/usr/bin/env bash
# Uso: scripts/run.sh <perfil> [porta] [instancia] [args extras...]
# Perfis: produtor | circulacao | comunicacao | multas | auditoria | estatisticas
# Ex.:   scripts/run.sh estatisticas 8086 est-2 --app.processamento-ms=500
set -e
PERFIL=${1:?informe o perfil}
PORTA=${2:-}
INST=${3:-$PERFIL}
ARGS=(--spring.profiles.active="$PERFIL" --app.instancia="$INST")
[ -n "$PORTA" ] && ARGS+=(--server.port="$PORTA")
exec java -jar "$(dirname "$0")/../target/biblioteca.jar" "${ARGS[@]}" "${@:4}"
