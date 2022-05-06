FROM python:3

WORKDIR /home/pulsar-client

COPY . .

RUN openssl genrsa -out private.pem 2048 && openssl rsa -in private.pem -pubout -out public.pem

