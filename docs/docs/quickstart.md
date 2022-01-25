# Download
***
- Download binary distro: [https://github.com/zendesk/maxwell/releases/download/v1.33.1/maxwell-1.33.1.tar.gz](https://github.com/zendesk/maxwell/releases/download/v1.33.1/maxwell-1.33.1.tar.gz)
- Sources and bug tracking is available on github: [https://github.com/zendesk/maxwell](https://github.com/zendesk/maxwell)

**curl**:
```
curl -sLo - https://github.com/zendesk/maxwell/releases/download/v1.33.1/maxwell-1.33.1.tar.gz \
       | tar zxvf -
cd maxwell-1.33.1
```

**docker**:

```
docker pull zendesk/maxwell
```

**homebrew**:

```
brew install maxwell
```

# Configure Mysql
***

*Server Config:* Ensure server_id is set, and that row-based replication is on.

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

*Permissions:* Maxwell needs permissions to act as a replica, and to write to the `maxwell` database.
```
mysql> CREATE USER 'maxwell'@'%' IDENTIFIED BY 'XXXXXX';
mysql> GRANT ALL ON maxwell.* TO 'maxwell'@'%';
mysql> GRANT SELECT, REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO 'maxwell'@'%';

# or for running maxwell locally:

mysql> CREATE USER 'maxwell'@'localhost' IDENTIFIED BY 'XXXXXX';
mysql> GRANT ALL ON maxwell.* TO 'maxwell'@'localhost';
mysql> GRANT SELECT, REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO 'maxwell'@'localhost';
```

# Run Maxwell
***

## Command line
```
bin/maxwell --user='maxwell' --password='XXXXXX' --host='127.0.0.1' --producer=stdout
```

## Docker
```
docker run -it --rm zendesk/maxwell bin/maxwell --user=$MYSQL_USERNAME \
    --password=$MYSQL_PASSWORD --host=$MYSQL_HOST --producer=stdout
```

## Kafka

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

## Kinesis

```
docker run -it --rm --name maxwell -v `cd && pwd`/.aws:/root/.aws zendesk/maxwell sh -c 'cp /app/kinesis-producer-library.properties.example /app/kinesis-producer-library.properties && echo "Region=$AWS_DEFAULT_REGION" >> /app/kinesis-producer-library.properties && bin/maxwell --user=$MYSQL_USERNAME --password=$MYSQL_PASSWORD --host=$MYSQL_HOST --producer=kinesis --kinesis_stream=$KINESIS_STREAM'
```

## Nats

```
bin/maxwell --user='maxwell' --password='XXXXXX' --host='127.0.0.1' \
    --producer=nats --nats_url=='0.0.0.0:4222'
```

## Google Cloud Pub/Sub

```
bin/maxwell --user='maxwell' --password='XXXXXX' --host='127.0.0.1' \
  --producer=pubsub --pubsub_project_id='$PUBSUB_PROJECT_ID' \
  --pubsub_topic='maxwell'
```

## RabbitMQ

```
bin/maxwell --user='maxwell' --password='XXXXXX' --host='127.0.0.1' \
    --producer=rabbitmq --rabbitmq_host='rabbitmq.hostname'
```

## Redis

```
bin/maxwell --user='maxwell' --password='XXXXXX' --host='127.0.0.1' \
    --producer=redis --redis_host=redis.hostname
```

## SNS

```
bin/maxwell --user='maxwell' --password='XXXXXX' --host='127.0.0.1' \
    --producer=sns --sns_topic=sns.topic --sns_attrs=database,table
```
