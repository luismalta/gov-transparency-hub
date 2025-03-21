#!/bin/sh
set -e  # Faz o script falhar caso qualquer comando falhe

echo "🔄 Gerando documentação do dbt..."
uv run dbt docs generate --project-dir=gov_transparency_hub/dbt_pipelines --profiles-dir=gov_transparency_hub/dbt_pipelines

echo "🚀 Iniciando Dagster Code Server..."
exec uv run dagster code-server start -h 0.0.0.0 -p 5001 -m gov_transparency_hub
