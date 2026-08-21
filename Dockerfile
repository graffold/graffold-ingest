FROM python:3.13-slim
WORKDIR /app
RUN pip install --no-cache-dir uv
COPY pyproject.toml uv.lock* README.md ./
COPY src/ src/
RUN uv sync --frozen --no-dev --extra llm
EXPOSE 8001
ENV INGEST_INTERNAL_MODE=false
CMD [".venv/bin/uvicorn", "graffold_ingest.api:app", "--host", "0.0.0.0", "--port", "8001"]
