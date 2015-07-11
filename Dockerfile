FROM jacobat/ruby:2.1.5-3

RUN apt-get update && apt-get install -y openjdk-7-jre curl

RUN mkdir /app
WORKDIR /app

RUN curl -sLo - https://github.com/zendesk/maxwell/releases/download/v0.9.0/maxwell-0.9.0.tar.gz | tar zxvf -

WORKDIR /app/maxwell-0.9.0

ADD REVISION /

CMD bin/maxwell --user=$MYSQL_USERNAME --password=$MYSQL_PASSWORD --host=$MYSQL_HOST --producer=kafka --kafka.bootstrap.servers=$KAFKA_HOST:$KAFKA_PORT
