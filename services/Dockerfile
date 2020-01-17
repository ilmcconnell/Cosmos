FROM python:3.7-alpine

RUN apk add ghostscript build-base jpeg-dev zlib-dev --no-cache

COPY requirements.txt /

RUN CFLAGS='-Wl,--strip-all' pip install -r /requirements.txt

