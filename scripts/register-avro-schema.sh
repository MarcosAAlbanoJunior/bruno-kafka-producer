#!/usr/bin/env bash
# Registra (ou reaproveita, se identico) um schema Avro/JSON/Protobuf no Confluent
# Schema Registry via REST, a partir de um arquivo local (ex: schemas/pagamentos.pedido.criado.v1-value.avsc).
#
# Uso:
#   ./scripts/register-avro-schema.sh <schema-registry-url> <subject> <arquivo-schema> [tipo] [usuario:senha]
#
# Exemplos:
#   ./scripts/register-avro-schema.sh http://localhost:8081 pedidos-value schemas/pagamentos.pedido.criado.v1-value.avsc
#   ./scripts/register-avro-schema.sh https://meu-registry:8081 pedidos-value schemas/pagamentos.pedido.criado.v1-value.avsc AVRO scruser:scrpass
#
# Requer: curl e jq.

set -euo pipefail

REGISTRY_URL="${1:?Uso: $0 <schema-registry-url> <subject> <arquivo-schema> [tipo] [usuario:senha]}"
SUBJECT="${2:?Faltou o subject (ex: pedidos-value)}"
SCHEMA_FILE="${3:?Faltou o caminho do arquivo de schema (.avsc/.json/.proto)}"
SCHEMA_TYPE="${4:-AVRO}"
AUTH="${5:-}"

if ! command -v jq >/dev/null 2>&1; then
  echo "Erro: este script precisa do 'jq' instalado (https://jqlang.org)." >&2
  exit 1
fi

if [[ ! -f "$SCHEMA_FILE" ]]; then
  echo "Erro: arquivo de schema nao encontrado: $SCHEMA_FILE" >&2
  exit 1
fi

# jq monta o JSON e escapa o conteudo do arquivo corretamente (evita erro manual de aspas/quebras de linha)
BODY=$(jq -n --rawfile schema "$SCHEMA_FILE" --arg schemaType "$SCHEMA_TYPE" \
  '{schema: $schema, schemaType: $schemaType}')

CURL_ARGS=(-sS -X POST
  -H "Content-Type: application/vnd.schemaregistry.v1+json"
  --data "$BODY")

if [[ -n "$AUTH" ]]; then
  CURL_ARGS=(-u "$AUTH" "${CURL_ARGS[@]}")
fi

CURL_ARGS+=("$REGISTRY_URL/subjects/$SUBJECT/versions")

echo "Registrando '$SCHEMA_FILE' ($SCHEMA_TYPE) no subject '$SUBJECT'..." >&2
RESPONSE=$(curl "${CURL_ARGS[@]}")
echo "$RESPONSE" | jq .

ID=$(echo "$RESPONSE" | jq -r '.id // empty')
if [[ -n "$ID" ]]; then
  echo "" >&2
  echo "Schema id: $ID" >&2
  echo "-> use em 'kafkaValueSchemaId' no environment do Bruno, ou como 'value_schema_id' no produce-via-rest-proxy.sh" >&2
fi
