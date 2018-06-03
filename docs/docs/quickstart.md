### Download
***
- Download binary distro: [https://github.com/zendesk/maxwell/releases/download/v1.14.7/maxwell-1.14.7.tar.gz](https://github.com/zendesk/maxwell/releases/download/v1.14.7/maxwell-1.14.7.tar.gz)
- Sources and bug tracking is available on github: [https://github.com/zendesk/maxwell](https://github.com/zendesk/maxwell)
- Obligatory copy/paste to terminal:

```
curl -sLo - https://github.com/zendesk/maxwell/releases/download/v1.14.7/maxwell-1.14.7.tar.gz \
       | tar zxvf -
cd maxwell-1.14.7
```

or get the docker image:

```
docker pull zendesk/maxwell
```

or on Mac OS X with homebrew installed:

```
brew install maxwell
```

### Configure Mysql
***

*Server Config:* Ensure your server_id is configured, and that row-based replication is turned on.

```
$ vi my.cnf

[mysqld]
server_id=1
log-bin=master
binlog_format=row
```


Or on a running server:

```
mysql> set global binlog_format=ROW;
mysql> set global binlog_row_image=FULL;
```

note: `binlog_format` is a session-based property.  You will need to shutdown all active connections to fully convert
to row-based replication.

*Permissions:* Maxwell needs permissions to store state in the database specified by the `schema_database` option (default `maxwell`).
```
mysql> GRANT ALL on maxwell.* to 'maxwell'@'%' identified by 'XXXXXX';
mysql> GRANT SELECT, REPLICATION CLIENT, REPLICATION SLAVE on *.* to 'maxwell'@'%';

# or for running maxwell locally:

mysql> GRANT SELECT, REPLICATION CLIENT, REPLICATION SLAVE on *.* to 'maxwell'@'localhost' identified by 'XXXXXX';
mysql> GRANT ALL on maxwell.* to 'maxwell'@'localhost';

```

### Run Maxwell
***

#### Command line
```
bin/maxwell --user='maxwell' --password='XXXXXX' --host='127.0.0.1' --producer=stdout
```

#### Docker
```
docker run -it --rm zendesk/maxwell bin/maxwell --user=$MYSQL_USERNAME \
    --password=$MYSQL_PASSWORD --host=$MYSQL_HOST --producer=stdout
```

#### Kafka

Boot kafka as described here:  [http://kafka.apache.org/documentation.html#quickstart](http://kafka.apache.org/documentation.html#quickstart), then:

```
bin/maxwell --user='maxwell' --password='XXXXXX' --host='127.0.0.1' \
   --producer=kafka --kafka.bootstrap.servers=localhost:9092 --kafka_topic=maxwell
```

(or docker):

```
docker run -it --rm zendesk/maxwell bin/maxwell --user=$MYSQL_USERNAME \
    --password=$MYSQL_PASSWORD --host=$MYSQL_HOST --producer=kafka \
    --kafka.bootstrap.servers=$KAFKA_HOST:$KAFKA_PORT --kafka_topic=maxwell
```

#### Kinesis

```
docker run -it --rm --name maxwell -v `cd && pwd`/.aws:/root/.aws zendesk/maxwell sh -c 'cp /app/kinesis-producer-library.properties.example /app/kinesis-producer-library.properties && echo "Region=$AWS_DEFAULT_REGION" >> /app/kinesis-producer-library.properties && bin/maxwell --user=$MYSQL_USERNAME --password=$MYSQL_PASSWORD --host=$MYSQL_HOST --producer=kinesis --kinesis_stream=$KINESIS_STREAM'
```

#### Google Cloud Pub/Sub

```
bin/maxwell --user='maxwell' --password='XXXXXX' --host='127.0.0.1' \
  --producer=pubsub --pubsub_project_id='$PUBSUB_PROJECT_ID' \
  --pubsub_topic='maxwell'
```

#### RabbitMQ

```
bin/maxwell --user='maxwell' --password='XXXXXX' --host='127.0.0.1' \
    --producer=rabbitmq --rabbitmq_host='rabbitmq.hostname'
```

#### Redis

```
bin/maxwell --user='maxwell' --password='XXXXXX' --host='127.0.0.1' \
    --producer=redis --redis_host=redis.hostname
```
