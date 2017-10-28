FROM maven:3.5-jdk-8
ENV MAXWELL_VERSION=1.10.8 KAFKA_VERSION=0.10.1.0
ENV DEBIAN_FRONTEND noninteractive

COPY . /workspace

RUN apt-get update \
    && apt-get upgrade -y \
    && apt-get install -y build-essential \
    && cd /workspace \
    && KAFKA_VERSION=$KAFKA_VERSION make package MAXWELL_VERSION=$MAXWELL_VERSION \
    && mkdir /app \
    && mv /workspace/target/maxwell-$MAXWELL_VERSION/maxwell-$MAXWELL_VERSION/* /app/ \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* /usr/share/doc/* /workspace/ \
    && echo "$MAXWELL_VERSION" > /REVISION

WORKDIR /app

CMD bin/maxwell --user=$MYSQL_USERNAME --password=$MYSQL_PASSWORD --host=$MYSQL_HOST --producer=kafka --kafka.bootstrap.servers=$KAFKA_HOST:$KAFKA_PORT $MAXWELL_OPTIONS