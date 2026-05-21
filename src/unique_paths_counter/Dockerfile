FROM python:3-alpine

RUN pip3 install pika

COPY common ./common
COPY unique_paths_counter .

ENTRYPOINT ["python3", "main.py"]
