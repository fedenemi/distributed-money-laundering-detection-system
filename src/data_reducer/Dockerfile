FROM python:3-alpine

RUN pip3 install pika

WORKDIR /app
COPY common ./common
COPY data_reducer ./data_reducer

ENV PYTHONPATH=/app:/app/common

ENTRYPOINT ["python3", "-m", "data_reducer.main"]
