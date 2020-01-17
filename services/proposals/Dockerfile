FROM python:3.7-alpine

# Install bash and vim
#RUN apk add bash vim

# Pillow requirements
#run apt install build-base jpeg-dev zlib-dev
run apk add build-base jpeg-dev zlib-dev --no-cache

COPY requirements.txt /

#RUN pip install -r /requirements.txt
RUN CFLAGS='-Wl,--strip-all' pip install -r /requirements.txt

COPY src/ /app
WORKDIR /app
