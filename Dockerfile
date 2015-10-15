FROM docker-registry.zende.sk/zendesk/ruby:2.1.6

RUN apt-get update && apt-get install -y openjdk-7-jdk curl
RUN apt-get install -y maven make build-essential -y

RUN mkdir /app
WORKDIR /app

ADD src /app/src
ADD bin /app/bin
ADD .settings /app/.settings
ADD pom.xml /app/

RUN mvn package -Dmaven.test.skip=true

ADD REVISION /

CMD bin/maxwell --user=$MYSQL_USERNAME --password=$MYSQL_PASSWORD --host=$MYSQL_HOST --producer=kafka --kafka.bootstrap.servers=$KAFKA_HOST:$KAFKA_PORT
