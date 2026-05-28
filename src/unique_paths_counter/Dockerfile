FROM python:3-alpine

RUN pip3 install pika

WORKDIR /app
COPY common ./common
COPY unique_paths_counter ./unique_paths_counter

ENV PYTHONPATH=/app:/app/common
    
ENTRYPOINT ["python3", "-m", "unique_paths_counter.main"]