FROM java:openjdk-7-jre
ENV MAXWELL_VERSION 1.5.2

RUN apt-get update && apt-get -y upgrade

RUN mkdir /app
WORKDIR /app

RUN curl -sLo - https://github.com/zendesk/maxwell/releases/download/v"$MAXWELL_VERSION"/maxwell-"$MAXWELL_VERSION".tar.gz \
  | tar --strip-components=1 -zxvf -

RUN echo "$MAXWELL_VERSION" > /REVISION
CMD bin/maxwell --user=$MYSQL_USERNAME --password=$MYSQL_PASSWORD --host=$MYSQL_HOST --producer=kafka --kafka.bootstrap.servers=$KAFKA_HOST:$KAFKA_PORT $MAXWELL_OPTIONS
