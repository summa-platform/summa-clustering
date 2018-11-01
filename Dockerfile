FROM ubuntu:18.04

RUN apt update && apt install -y python3-pip curl
RUN pip3 install --upgrade pip
RUN pip3 install gensim==3.4.0 aio-pika==0.21.0

WORKDIR /opt/app

COPY . /opt/app/

VOLUME /opt/app/state

ENV LANG C.UTF-8
ENV PYTHONUNBUFFERED y

ENTRYPOINT ["/opt/app/rabbitmq.py"]
