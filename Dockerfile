FROM openjdk:8u151-jdk-alpine
ENV MAXWELL_VERSION=1.14.4 KAFKA_VERSION=0.11.0.1

COPY . /workspace

RUN apk --no-cache add --virtual .build-dependencies make maven \
    && apk --no-cache add java-snappy-native \
    && cd /workspace \
    && KAFKA_VERSION=$KAFKA_VERSION make package MAXWELL_VERSION=$MAXWELL_VERSION \
    && mkdir /app \
    && mv /workspace/target/maxwell-$MAXWELL_VERSION/maxwell-$MAXWELL_VERSION/* /app/ \
    && apk del .build-dependencies \
    && rm -rf /tmp/* /var/tmp/* /usr/share/doc/* /workspace/ /root/.m2/ \
    && echo "$MAXWELL_VERSION" > /REVISION

WORKDIR /app

CMD [ "/bin/bash", "-c", "bin/maxwell --user=$MYSQL_USERNAME --password=$MYSQL_PASSWORD --host=$MYSQL_HOST --producer=kafka --kafka.bootstrap.servers=$KAFKA_HOST:$KAFKA_PORT $MAXWELL_OPTIONS" ]
