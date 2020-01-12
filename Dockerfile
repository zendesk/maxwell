FROM maven:3.6-jdk-8
ENV MAXWELL_VERSION=1.24.0 KAFKA_VERSION=2.3.0

RUN apt-get update \
    && apt-get -y upgrade \
    && apt-get install -y make

# prime so we can have a cached image of the maven deps
COPY pom.xml /tmp
RUN cd /tmp && mvn dependency:resolve

COPY . /workspace
RUN cd /workspace \
    && KAFKA_VERSION=$KAFKA_VERSION make package MAXWELL_VERSION=$MAXWELL_VERSION \
    && mkdir /app \
    && mv /workspace/target/maxwell-$MAXWELL_VERSION/maxwell-$MAXWELL_VERSION/* /app/ \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* /usr/share/doc/* /workspace/ /root/.m2/ \
    && echo "$MAXWELL_VERSION" > /REVISION

WORKDIR /app

CMD [ "/bin/bash", "-c", "bin/maxwell-docker" ]
