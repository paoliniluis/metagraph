FROM python:3.11.4-alpine

COPY . /app/

RUN pip install -r /app/requirements.txt