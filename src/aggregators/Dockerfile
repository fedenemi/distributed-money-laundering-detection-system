FROM python:3-alpine

ENV PYTHONPATH=/app:/app/common
RUN pip3 install pika

COPY common /app/common
COPY aggregators/aggregator.py /app/aggregator.py
COPY aggregators/splitter.py   /app/splitter.py

# Entrypoint por defecto es aggregator, pero el compose puede sobrescribirlo
ENTRYPOINT ["python3", "/app/aggregator.py"]