FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY src /app/src
COPY README.md /app/README.md

# Persist DB in a mounted volume by default.
ENV SQLITE_PATH=/data/bot.db

RUN useradd --create-home --shell /usr/sbin/nologin botuser && \
    mkdir -p /data && chown -R botuser:botuser /app /data

USER botuser

CMD ["python", "-m", "src.main"]
