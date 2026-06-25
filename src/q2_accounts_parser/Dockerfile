FROM python:3-alpine

RUN pip3 install pika

WORKDIR /app
COPY common ./common
COPY q2_accounts_parser ./q2_accounts_parser

ENV PYTHONPATH=/app:/app/common

ENTRYPOINT ["python3", "-m", "q2_accounts_parser.q2_accounts_parser"]