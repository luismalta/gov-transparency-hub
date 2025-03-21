FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim


COPY gov_transparency_hub /app/gov_transparency_hub
COPY pyproject.toml /app/pyproject.toml
COPY uv.lock /app/uv.lock
COPY entrypoint.sh /entrypoint.sh

RUN chmod +x /entrypoint.sh

WORKDIR /app
RUN uv sync --frozen

EXPOSE 5001

ENTRYPOINT ["/entrypoint.sh"]