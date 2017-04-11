FROM java:openjdk-7
ENV MAXWELL_VERSION 1.8.2
ENV KAFKA_VERSION 0.10.1.0

RUN apt-get update && apt-get -y upgrade

RUN apt-get install build-essential ruby -y

COPY . /workspace
WORKDIR /workspace

RUN KAFKA_VERSION=$KAFKA_VERSION make package MAXWELL_VERSION=$MAXWELL_VERSION

WORKDIR /workspace/target/maxwell-$MAXWELL_VERSION/

RUN mkdir /app \
  && mv ./* /app/

WORKDIR /app

RUN echo "$MAXWELL_VERSION" > /REVISION
CMD bin/maxwell --user=$MYSQL_USERNAME --password=$MYSQL_PASSWORD --host=$MYSQL_HOST --producer=kafka --kafka.bootstrap.servers=$KAFKA_HOST:$KAFKA_PORT $MAXWELL_OPTIONS
