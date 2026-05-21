FROM python:3-alpine

RUN pip3 install pika

COPY common ./common
COPY aggregators .

ENTRYPOINT ["python3", "aggregator.py"]
