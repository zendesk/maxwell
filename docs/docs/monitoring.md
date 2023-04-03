# Monitoring
***
Maxwell exposes certain metrics through either its base logging mechanism, JMX, HTTP or by push to Datadog. This is configurable through commandline options
or the `config.properties` file. These can provide insight into system health.

# Metrics
***
All metrics are prepended with the configured `metrics_prefix.`

metric                         | description
-------------------------------|-------------------------------------
**Counters**
`messages.succeeded`           | count of messages that were successfully sent to Kafka
`messages.failed`              | count of messages that failed to send to Kafka
`row.count`                    | a count of rows that have been processed from the binlog. note that not every row results in a message being sent to Kafka.
**Meters**
`messages.succeeded.meter`     | a measure of the rate at which messages were successfully sent to Kafka
`messages.failed.meter`        | a measure of the rate at which messages failed to send Kafka
`row.meter`                    | a measure of the rate at which rows arrive to Maxwell from the binlog connector
**Gauges**
`replication.lag`              | the time elapsed between the database transaction commit and the time it was processed by Maxwell
`inflightmessages.count`       | the number of messages that are currently in-flight (awaiting acknowledgement from the destination, or ahead of messages which are)
**Timers**
`message.publish.time`         | the time it took to send a given record to Kafka
`message.publish.age`          | the time between an event occurring on the DB and being published to kafka. Note: since MySQL timestamps are accurate to the second, this is only accurate to +/- 500ms.
`replication.queue.time`       | the time it took to enqueue a given binlog event for processing

# HTTP Endpoints
***
When the HTTP server is enabled the following endpoints are exposed:

| endpoint       | description                                                                    |
|:---------------|:-------------------------------------------------------------------------------|
| `/metrics`     | GET all metrics as JSON                                                        |
| `/prometheus`  | GET all metrics as Prometheus format                                           |
| `/healthcheck` | run Maxwell's healthchecks.  Considered unhealthy if &gt;0 messages have failed in the last 15 minutes. |
| `/ping`        | a simple ping test, responds with `pong`                                       |
| `/diagnostics` | for kafka, send a fake message that measures the client to server latency      |

## Custom Health Check
Similar to the custom producer, developers can provide their own implementation of a health check.

In order to register your custom health check, you must implement the `MaxwellHealthCheckFactory` interface, which is responsible for creating your custom `MaxwellHealthCheck`. Next, set the `custom_health.factory` configuration property to your `MaxwellHealthCheckFactory`'s fully qualified class name. Then add the custom `MaxwellHealthCheckFactory` JAR and all its dependencies to the $MAXWELL_HOME/lib directory.

Custom health check factory and health check examples can be found here: [https://github.com/zendesk/maxwell/tree/master/src/example/com/zendesk/maxwell/example/maxwellhealthcheckfactory](https://github.com/zendesk/maxwell/tree/master/src/example/com/zendesk/maxwell/example/maxwellhealthcheckfactory)

# JMX Configuration
***
Standard configuration is either via commandline args or the `config.properties` file. However, when exposing JMX metrics
additional configuration is required to allow remote access. In this case Maxwell makes use of the `JAVA_OPTS` environment variable.
To make use of this set `JAVA_OPTS` before starting Maxwell.

The following is an example which allows remote access with no authentication and insecure connections.

```
export JAVA_OPTS="-Dcom.sun.management.jmxremote \
-Dcom.sun.management.jmxremote.port=9010 \
-Dcom.sun.management.jmxremote.local.only=false \
-Dcom.sun.management.jmxremote.authenticate=false \
-Dcom.sun.management.jmxremote.ssl=false \
-Djava.rmi.server.hostname=SERVER_IP_ADDRESS"
```
