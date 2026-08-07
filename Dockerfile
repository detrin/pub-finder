FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN pip install --no-cache-dir uv==0.11.2 \
    && uv sync --locked --no-dev --no-install-project
COPY . .

EXPOSE 3000

CMD ["/app/.venv/bin/python", "-m", "backend"]
