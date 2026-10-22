#!/usr/bin/env bash
# Gera o .env a partir do .env.example. Preenche com valores aleatórios só as chaves
# vazias e acrescenta chaves novas do exemplo; nunca altera um valor já preenchido,
# porque trocar a senha de um banco já inicializado quebraria o acesso a ele.
set -euo pipefail

ENV_FILE="${ENV_FILE:-.env}"
EXAMPLE="${EXAMPLE:-.env.example}"

[ -f "$ENV_FILE" ] || cp "$EXAMPLE" "$ENV_FILE"

# Chaves do exemplo que ainda não existem no .env entram com o valor do exemplo.
while IFS= read -r line || [ -n "$line" ]; do
  if [[ "$line" =~ ^([A-Z0-9_]+)= ]] && ! grep -q "^${BASH_REMATCH[1]}=" "$ENV_FILE"; then
    echo "$line" >> "$ENV_FILE"
  fi
done < "$EXAMPLE"

generate() {
  case "$1" in
    # O Airflow exige 32 bytes em base64 URL-safe.
    AIRFLOW_FERNET_KEY) openssl rand -base64 32 | tr '+/' '-_' ;;
    *_KEY | *_SECRET) openssl rand -hex 32 ;;
    *) openssl rand -hex 16 ;;
  esac
}

tmp="$(mktemp)"
filled=0
while IFS= read -r line || [ -n "$line" ]; do
  if [[ "$line" =~ ^([A-Z0-9_]+)=$ ]]; then
    echo "${BASH_REMATCH[1]}=$(generate "${BASH_REMATCH[1]}")" >> "$tmp"
    filled=$((filled + 1))
  else
    echo "$line" >> "$tmp"
  fi
done < "$ENV_FILE"
mv "$tmp" "$ENV_FILE"
chmod 600 "$ENV_FILE"

echo "$ENV_FILE: $filled valor(es) gerado(s); valores existentes mantidos."
