#Docker image of MaxWell Can Support Amd64 and Arm64/v8
FROM openjdk:11-slim
#FROM openjdk:11
ENV MAXWELL_VERSION=1.33.0
ENV MAXWELL_FILE maxwell-1.33.0.tar.gz
ENV MAXWELL_DOWNLOAD_PATH "https://github.com/zendesk/maxwell/releases/download/v1.33.0/maxwell-1.33.0.tar.gz"

RUN apt-get install wget; \
    cd /temp; \
    wget -O $MAXWELL_FILE $MAXWELL_DOWNLOAD_PATH; \
    mkdir /app; \
    tar --extract --file $MAXWELL_FILE --directory /app --strip-components 1 --no-same-owner; \
    apt-get clean;\
    rm -rf /temp

WORKDIR /app

CMD [ "/bin/bash", "-c", "bin/maxwell-docker" ]