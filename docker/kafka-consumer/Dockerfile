FROM python:3.12-slim

WORKDIR /app

COPY ingestion/kafka/consumer.py .

RUN pip install --no-cache-dir confluent-kafka==2.6.1

CMD ["python", "consumer.py"]