FROM java:openjdk-7-jre
ENV MAXWELL_VERSION 1.5.2
ENV ENDPOINT http://requestb.in/xr9aegxr

RUN mkdir /app

COPY target/maxwell-"$MAXWELL_VERSION".tar.gz .
RUN tar -zxf maxwell-"$MAXWELL_VERSION".tar.gz -C /app

RUN echo "$MAXWELL_VERSION" > /REVISION

WORKDIR /app/maxwell-"$MAXWELL_VERSION"
CMD ./bin/maxwell --user=root --password=$MYSQL_ENV_MYSQL_ROOT_PASSWORD --host=$MYSQL_PORT_3306_TCP_ADDR --producer=httppost --httppost_endpoint="$ENDPOINT"
