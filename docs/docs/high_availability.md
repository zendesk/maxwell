# High Availabilty

As v1.29.1, Maxwell contains experiemental (alpha quality) client-side HA.
Support for performing leader elections is done via
[jgroups-raft](https://github.com/belaban/jgroups-raft).

## Getting started

First, copy `raft.xml.example` to `raft.xml`.  Edit to your liking, paying attention to:

```
    <raft.RAFT members="A,B,C" raft_id="${raft_id:undefined}"/>
```

Note that because we are using a RAFT-based leader election, we will have to spin up at least
3 maxwell client nodes.


Now start each of your HA maxwell nodes like this:

```bash
host1: $ bin/maxwell --ha --raft_member_id=A
host2: $ bin/maxwell --ha --raft_member_id=B
host3: $ bin/maxwell --ha --raft_member_id=C
```

if all goes well, the 3 nodes will communicate via multicast/UDP, elect one to
be the cluster leader, and away you will go.  If one node is terminated or
partitioned, a new election will be held to replace it.


## Getting deeper

More advanced (especially inter-DC) configurations may be implemented by
editing `raft.xml`; you'll probably need to get the nodes to communicate with
each other via TCP instead of UDP, and maybe tunnel through a firewall or two,
good stuff like that.  It's of course out of scope for this document, so
[check out the jgroups
documentation](http://www.jgroups.org/manual/html/user-advanced.html), but if
you come up with something good drop me a line.

## Common problems

Something I encountered right out of the gate was this:

```
12:37:53,135 WARN  UDP - failed to join /224.0.75.75:7500 on utun0: java.net.SocketException: Can't assign requested address
```

which can be worked around by forcing the JVM onto an ipv4 stack:

```
JAVA_OPTS="-Djava.net.preferIPv4Stack=true" bin/maxwell --ha --raft_member_id=B
```

# High Availabilty on Zookeeper

High availability through zookeeper

## Getting started
Prepare two or more servers to serve as the maxwell host server and a zookeeper cluster.  (The maxwell host server and a zookeeper cluster can communicate.)

Example Running Scripts:

```
    bin/maxwell --log_level='INFO' --user='<user>' --password='<passwd>' --host='<host>' --producer=stdout --client_id='<client_id>' --ha='zookeeper' --zookeeper_server ='<host1:port>,<host2:port>,<host3:port>'
```

Run the preceding command on each maxwell host.

Get which host is the leader script Example:
```
    bin/maxwell-leaders --ha='zookeeper' --zookeeper_server ='<host1:port>,<host2:port>,<host3:port>' --client_id='<client_id>'
```
You can get:
```
    [INFO] MaxwellLeaders: clientID:<clientID>:leaders now are -> <leader host>
```

## Getting deeper
If a timeout error occurs between the maxwell host and the zookeeper cluster or the connection is abnormal due to network instability, you can set the following parameters:
```
--zookeeper_session_timeout_ms=<session timeout duration> 
--zookeeper_connection_timeout_ms=<internal default wait time for the client to establish a connection with the zk> 
--zookeeper_max_retries=<number of retries>
--zookeeper_retry_wait_ms=<retry time interval>
```

