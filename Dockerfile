FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim


COPY gov_transparency_hub /app/gov_transparency_hub
COPY pyproject.toml /app/pyproject.toml
COPY uv.lock /app/uv.lock


WORKDIR /app
RUN uv sync --frozen

EXPOSE 5001

CMD ["uv", "run", "dagster", "code-server", "start", "-h", "0.0.0.0", "-p", "5001", "-m", "gov_transparency_hub"]