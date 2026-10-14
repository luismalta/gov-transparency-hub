#!/bin/sh
set -e  # Faz o script falhar caso qualquer comando falhe

# dbt_packages/ and target/ are gitignored: install packages and build the
# manifest that @dbt_assets loads. parse does not connect to the warehouse.
echo "🔄 Preparando o projeto dbt..."
uv run dbt deps --project-dir=gov_transparency_hub/dbt_pipelines --profiles-dir=gov_transparency_hub/dbt_pipelines
uv run dbt parse --project-dir=gov_transparency_hub/dbt_pipelines --profiles-dir=gov_transparency_hub/dbt_pipelines

echo "🚀 Iniciando Dagster Code Server..."
exec uv run dagster code-server start -h 0.0.0.0 -p 5001 -m gov_transparency_hub
