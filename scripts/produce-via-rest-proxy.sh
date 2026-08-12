#!/usr/bin/env bash
# Publica UMA mensagem em um topico Kafka via Confluent REST Proxy (ou compativel,
# ex. Karapace REST), usando um schema ja registrado no Schema Registry (por id).
#
# IMPORTANTE: curl fala HTTP, e o protocolo nativo do Kafka e binario sobre TCP - nao
# da pra "curlar" direto num broker. Este script assume que existe um REST Proxy
# rodando na frente do cluster. Se voce nao tem um REST Proxy, use a collection do
# Bruno (fala kafkajs nativamente, sem precisar dessa camada extra) - ver
# "Send Kafka Message.bru" na raiz deste repo.
#
# Uso:
#   ./scripts/produce-via-rest-proxy.sh <rest-proxy-url> <topico> <schema-id> <valor-json|@arquivo.json> [usuario:senha]
#
# Exemplos:
#   ./scripts/produce-via-rest-proxy.sh http://localhost:8082 pedidos 1 '{"orderId":"12345","status":"created"}'
#   ./scripts/produce-via-rest-proxy.sh http://localhost:8082 pedidos 1 @payload.json restuser:restpass
#
# Requer: curl e jq.

set -euo pipefail

REST_PROXY_URL="${1:?Uso: $0 <rest-proxy-url> <topico> <schema-id> <valor-json|@arquivo.json> [usuario:senha]}"
TOPIC="${2:?Faltou o nome do topico}"
SCHEMA_ID="${3:?Faltou o schema id (veja register-avro-schema.sh)}"
VALUE_INPUT="${4:?Faltou o valor da mensagem (JSON inline ou @arquivo.json)}"
AUTH="${5:-}"

if ! command -v jq >/dev/null 2>&1; then
  echo "Erro: este script precisa do 'jq' instalado (https://jqlang.org)." >&2
  exit 1
fi

if [[ "$VALUE_INPUT" == @* ]]; then
  VALUE_FILE="${VALUE_INPUT:1}"
  if [[ ! -f "$VALUE_FILE" ]]; then
    echo "Erro: arquivo de valor nao encontrado: $VALUE_FILE" >&2
    exit 1
  fi
  VALUE_JSON=$(cat "$VALUE_FILE")
else
  VALUE_JSON="$VALUE_INPUT"
fi

BODY=$(jq -n --argjson schemaId "$SCHEMA_ID" --argjson value "$VALUE_JSON" \
  '{value_schema_id: $schemaId, records: [{value: $value}]}')

CURL_ARGS=(-sS -X POST
  -H "Content-Type: application/vnd.kafka.avro.v2+json"
  -H "Accept: application/vnd.kafka.v2+json"
  --data "$BODY")

if [[ -n "$AUTH" ]]; then
  CURL_ARGS=(-u "$AUTH" "${CURL_ARGS[@]}")
fi

CURL_ARGS+=("$REST_PROXY_URL/topics/$TOPIC")

echo "Publicando no topico '$TOPIC' via REST Proxy (schema id $SCHEMA_ID)..." >&2
curl "${CURL_ARGS[@]}" | jq .
