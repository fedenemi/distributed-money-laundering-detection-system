FROM python:3-alpine

RUN apk add --no-cache docker-cli docker-cli-compose curl && \
    pip install --no-cache-dir pyyaml docker

WORKDIR /app
COPY chaos_monkey.py /app/

CMD ["python3", "-u", "chaos_monkey.py"]