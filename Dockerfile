FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY schemas/ schemas/
COPY src/ src/

ENV PYTHONPATH=/app/src
ENV LOG_LEVEL=INFO

ENTRYPOINT ["python"]
CMD ["src/producer.py"]
