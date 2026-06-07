FROM python:3-alpine

RUN pip3 install pika

COPY common ./common
COPY bank_name_adder .

ENTRYPOINT ["python3", "bank_name_adder.py"]