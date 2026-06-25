FROM python:3.12-alpine

RUN pip install pika docker

WORKDIR /app

COPY common ./common
COPY monitor .

ENTRYPOINT ["python3", "monitor.py"]