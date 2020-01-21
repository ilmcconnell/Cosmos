#FROM python:3.7
FROM iaross/cosmos_service_base


# Install bash and vim
#RUN apk add bash vim
RUN apk add --update --no-cache build-base libstdc++ \
        libpng libpng-dev \
        freetype freetype-dev

# Pillow requirements
#run apt install build-base jpeg-dev zlib-dev

COPY requirements.txt /

#For Debugging purpose
#RUN apt-get update && apt-get -y install vim

RUN pip install -r /requirements.txt

COPY src/ /app
WORKDIR /app
