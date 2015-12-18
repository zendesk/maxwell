FROM docker-registry.zende.sk/zendesk/ruby:2.1.6

RUN apt-get update && apt-get install -y openjdk-7-jre curl

RUN mkdir /app
WORKDIR /app

RUN curl -sLo - https://github.com/zendesk/maxwell/releases/download/v0.16.2-RC1/maxwell-0.16.2-RC1.tar.gz \
  | tar --strip-components=1 -zxvf -

ADD REVISION /

CMD bin/maxwell --user=$MYSQL_USERNAME --password=$MYSQL_PASSWORD --host=$MYSQL_HOST --producer=kafka --kafka.bootstrap.servers=$KAFKA_HOST:$KAFKA_PORT
