FROM maven:3.9.9-eclipse-temurin-23 AS builder
ENV MAXWELL_VERSION=1.43.0 KAFKA_VERSION=1.0.0


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
FROM eclipse-temurin:23-jre-noble

RUN apt-get update \
    && apt-get -y upgrade

COPY --from=builder /app /app
# COPY --from=builder /REVISION /REVISION

WORKDIR /app

RUN echo "$MAXWELL_VERSION" > /REVISION
#USER 1000


RUN apt-get update && apt-get install -y --no-install-recommends wget unzip procps python3-pip htop
# RUN pipx install magic-wormhole

ARG ASYNC_PROFILER_VERSION=2.9
RUN wget https://github.com/jvm-profiling-tools/async-profiler/releases/download/v${ASYNC_PROFILER_VERSION}/async-profiler-${ASYNC_PROFILER_VERSION}-linux-x64.tar.gz -O /tmp/async-profiler.tar.gz \
    && tar -xzf /tmp/async-profiler.tar.gz -C /opt \
    && rm /tmp/async-profiler.tar.gz
ENV ASYNC_PROFILER_HOME=/opt/async-profiler-${ASYNC_PROFILER_VERSION}-linux-x64
ENV PATH="$PATH:${ASYNC_PROFILER_HOME}"

CMD [ "/bin/bash", "-c", "bin/maxwell-docker" ]
