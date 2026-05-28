FROM python:3-alpine

RUN pip3 install pika

WORKDIR /app
COPY common ./common
COPY paths_creator ./paths_creator

ENV PYTHONPATH=/app:/app/common

ENTRYPOINT ["python3", "-m", "paths_creator.main"]