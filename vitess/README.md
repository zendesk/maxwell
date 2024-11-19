# Vitess Support for Maxwell

This directory contains a set of scripts and configuration files to help develop
and test Vitess support in Maxwell.

## Starting Vitess Test Server

The easiest way to develop and test Vitess clients is by running the `vttestserver`
Docker image. It contains all of the components of Vitess + a MySQL instance configured
in a way, that mimics a real Vitess cluster close enough for most development purposes.

See https://vitess.io/docs/13.0/get-started/vttestserver-docker-image/ for more details.

For developing Maxwell support for Vitess, you can run run vttestserver using the
provided script:

```
$ vitess/01-start-vitess.sh
```

The script will use Docker or Podman to start vttestserver and expose its gRPC and MySQL ports
so that Maxwell can connect to those.

## Running Maxwell against Vitess

To start Maxwell against the provided vttestserver cluster, you can use the provided script:

```
$ vitess/02-start-maxwell.sh
```
The script will build Maxwell and then start it with the provided properties file
(`vitess/config.properties`). The properties file configures the following:

1. Maxwell's schema is stored in a dedicated Vitess keyspace called `maxwell`.
2. Maxwell follows all changes in a Vitess keyspace called `app_shard`.

## Connecting to Vitess with a MySQL client

To play with the provided vttestserver cluster, you can connect to it using any MySQL client.
The test server does not require any credentials.

Here is an example command:

```
mysql -h 127.0.0.1 -P 33577
```

The MySQL interface in vtgate will expose multiple keyspaces, which you can read and write
into as usual.

## Connecting to VStream API using a gRPC client

If you would like to experiment with the gRPC APIs exposed by Vitess, you can use any gRPC
client and connect to `localhost:33575`.

Here is an example [gRPCurl](https://github.com/fullstorydev/grpcurl) command for following all
changes made to the `app_shard` keyspace:

```
grpcurl -plaintext -d '{"vgtid":{"shard_gtids":[{"keyspace":"app_shard", "gtid":"current"}]}}' localhost:33575 vtgateservice.Vitess.VStream
```

This will run the VStream API and send all events to console as a JSON stream.

## Cleaning up after testing

When you're done working with the vttestserver cluster, you may want to run the script provided to
clean up any remaining Docker containers:

```
$ vitess/03-cleanup-vitess.sh
```
