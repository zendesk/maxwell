FROM maven:3.8-jdk-11 as builder
ENV MAXWELL_VERSION=1.39.5 KAFKA_VERSION=1.0.0

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

# Build clean image with non-root priveledge
FROM openjdk:11-jdk-slim

RUN apt-get update \
    && apt-get -y upgrade

COPY --from=builder /app /app
COPY --from=builder /REVISION /REVISION

WORKDIR /app

RUN useradd -u 1000 maxwell -d /app
RUN chown 1000:1000 /app

USER 1000

CMD [ "/bin/bash", "-c", "bin/maxwell-docker" ]
