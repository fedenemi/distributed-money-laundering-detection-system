FROM python:3-alpine

RUN pip3 install pika requests

COPY common ./common
COPY money_converter/money_converter.py .
COPY money_converter/money_converter_api.py .
COPY money_converter/btc_rates.csv .

ENTRYPOINT ["python3", "money_converter.py"]
