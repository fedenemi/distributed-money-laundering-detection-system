FROM python:3-alpine

RUN pip3 install pika

COPY common ./common
COPY graph_paths .

# Non existent file added in order to specify entrypoint in compose
ENTRYPOINT ["python3", "graph_paths.py"]
