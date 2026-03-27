FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml .
RUN pip install --upgrade pip && pip install .
COPY . .

EXPOSE 3000

CMD ["python", "-m", "backend"]
