### Download
***
- Download binary distro: [https://github.com/zendesk/maxwell/releases/download/v1.12.0/maxwell-1.12.0.tar.gz](https://github.com/zendesk/maxwell/releases/download/v1.12.0/maxwell-1.12.0.tar.gz)
- Sources and bug tracking is available on github: [https://github.com/zendesk/maxwell](https://github.com/zendesk/maxwell)
- Obligatory copy/paste to terminal:

```
curl -sLo - https://github.com/zendesk/maxwell/releases/download/v1.12.0/maxwell-1.12.0.tar.gz \
       | tar zxvf -
cd maxwell-1.12.0
```

or get the docker image:

```
docker pull zendesk/maxwell
```

### Row based replication
***
Maxwell can only operate if row-based replication is on.

```
$ vi my.cnf

[mysqld]
server-id=1
log-bin=master
binlog_format=row
```

Or on a running server:

```
mysql> set global binlog_row_image=FULL;
```

*note*: When changing the binlog format on a running server, currently connected mysql clients will generate binlog entries in STATEMENT format, leading
to odd results.  In order to change to row-based replication in runtime, you must reconnect all active clients to the server.

### Mysql permissions
***
Maxwell stores all the state it needs within the mysql server itself, in the database called specified by the _schema_database_ option. By default the database is named `maxwell`.
```
mysql> GRANT ALL on maxwell.* to 'maxwell'@'%' identified by 'XXXXXX';
mysql> GRANT SELECT, REPLICATION CLIENT, REPLICATION SLAVE on *.* to 'maxwell'@'%';

# or for running maxwell locally:

mysql> GRANT SELECT, REPLICATION CLIENT, REPLICATION SLAVE on *.* to 'maxwell'@'localhost' identified by 'XXXXXX';
mysql> GRANT ALL on maxwell.* to 'maxwell'@'localhost';

```

### STDOUT producer
***
Useful for smoke-testing the thing.

```
bin/maxwell --user='maxwell' --password='XXXXXX' --host='127.0.0.1' --producer=stdout
```

If all goes well you'll see maxwell replaying your inserts:
```
mysql> insert into test.maxwell set id = 5, daemon = 'firebus!  firebus!';
Query OK, 1 row affected (0.04 sec)

(maxwell)
{"table":"maxwell","type":"insert","data":{"id":5,"daemon":"firebus!  firebus!"},"ts": 123456789}
```

### Docker

```
docker run -it --rm zendesk/maxwell bin/maxwell --user=$MYSQL_USERNAME --password=$MYSQL_PASSWORD --host=$MYSQL_HOST --producer=stdout
```

If you're using a virtualbox-docker setup on OSX, you'll need to ensure that:

- You pass the proper docker IP address as `$MYSQL_HOST`.
- You will want to use the non-localhost way of granting permissions to
  maxwell's mysql user.
- If you are specifying any files either for `config` location or for output of
  `file` producer then make sure you are explicitly sharing volumes/files
  present on the host. E.g. the above docker command then becomes,

```
docker run -v /Users:/Users -it --rm zendesk/maxwell bin/maxwell --config=/Users/demo/maxwell/config/basic.properties
```

(make a note of the `-v` option).

### Kafka producer
***
Boot kafka as described here:  [http://kafka.apache.org/documentation.html#quickstart](http://kafka.apache.org/documentation.html#quickstart), then:

```
bin/maxwell --user='maxwell' --password='XXXXXX' --host='127.0.0.1' \
   --producer=kafka --kafka.bootstrap.servers=localhost:9092
```

(or docker):

```
docker run -it --rm zendesk/maxwell bin/maxwell --user=$MYSQL_USERNAME --password=$MYSQL_PASSWORD --host=$MYSQL_HOST --producer=kafka --kafka.bootstrap.servers=$KAFKA_HOST:$KAFKA_PORT
```

This will start writing to the topic "maxwell".


### Kinesis Producer, docker
***

```
docker run -it --rm --name maxwell -v `cd && pwd`/.aws:/root/.aws zendesk/maxwell sh -c 'cp /app/kinesis-producer-library.properties.example /app/kinesis-producer-library.properties && echo "Region=$AWS_DEFAULT_REGION" >> /app/kinesis-producer-library.properties && bin/maxwell --user=$MYSQL_USERNAME --password=$MYSQL_PASSWORD --host=$MYSQL_HOST --producer=kinesis --kinesis_stream=$KINESIS_STREAM'
```

### Google Cloud Pub/Sub Producer

```
bin/maxwell --user='maxwell' --password='XXXXXX' --host='127.0.0.1' \
  --producer=pubsub --pubsub_project_id='$PUBSUB_PROJECT_ID' \
  --pubsub_topic='maxwell'
```

(or docker):

```
docker run -it --rm zendesk/maxwell bin/maxwell --user=$MYSQL_USERNAME --password=$MYSQL_PASSWORD --host=$MYSQL_HOST --producer=pubsub --pubsub_project_id='$PUBSUB_PROJECT_ID' --pubsub_topic='maxwell'
```

### RabbitMQ Producer

```
bin/maxwell --user='maxwell' --password='XXXXXX' --host='127.0.0.1' \
    --producer=rabbitmq --rabbitmq_host='rabbitmq.hostname'
```

### Redis Producer

```
bin/maxwell --user='maxwell' --password='XXXXXX' --host='127.0.0.1' \
    --producer=redis --redis_host=redis.hostname
```

(or docker):

```
docker run -it --rm zendesk/maxwell bin/maxwell --user=$MYSQL_USERNAME --password=$MYSQL_PASSWORD --host=$MYSQL_HOST --producer=redis --redis_host=$REDIS_HOST
```
