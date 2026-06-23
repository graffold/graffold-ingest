FROM python:3.13-slim
WORKDIR /app
RUN pip install --no-cache-dir uv
COPY pyproject.toml uv.lock* README.md ./
COPY src/ src/
RUN uv sync --no-dev
EXPOSE 8001
CMD [".venv/bin/uvicorn", "graffold_ingest.api:app", "--host", "0.0.0.0", "--port", "8001"]
