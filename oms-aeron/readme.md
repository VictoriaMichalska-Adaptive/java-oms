
# Building an Aeron OMS - WORK IN PROGRESS

Combining your knowledge of a Java OMS + Vertx, you will now implemenent Aeron into your OMS.

## Starter Topics:
- Distributed Systems
- RAFT Consensus
- Log Replication
- Encoding/Decoding Buffer Messaging
- Ingress/Egress
- Correlation IDs

## Main Aeron Tasks

### Task 1: Adding OMS Business Logic to your Cluster

You will want to execute the OMS business logic in your cluster, and therefore it should be registered in your nodes.

When a new Aeron Cluster node has started, it should have a Orderbook implementation within.

When the Cluster starts receiving ingress messages from a client, based on the ingress message it can then execute according business logic.

### Task 2: Implement Ingress Logic (Client)

To communicate with our Cluster, we need to first send a Ingress message from our Aeron client.

To send a single ingress message to our cluster:
- Encode a Buffer that contains our request
- Offer buffer to our cluster

You should implement the appropriate functions so we can interact with the business logic in our cluster.

Using our Ingress Sender, at a later point we can use this with a websocket (Vertx) to have a way of interfacing with our cluster using websocket requests.

### Task 3: Implement Ingress receiving logic (Cluster)

A cluster will receive ingress messages that require different business logic in the cluster to be executed.

You should find a solution to route ingress messages to appropriate logic

- Decode ingress buffer
- Execute appropriate business logic

#### Obstacle Topics:
- Messaging Demultiplexing - How do we route to cluster business logic based on ingress message?

### Task 4: Implement Egress sending logic (Cluster)

Once we have received ingress and executed the appropriate business logic, we will want to send a egress response back to our client so that it knows logic has executed.

To send a single egress message to our client:
- Encode a Buffer that contains our response
- Offer buffer to the Aeron client that made the request

### Task 5: Implement Egress receiving logic (Client)

This task is similar task 3, however instead you will be routing egress messages received from the cluster.

You should find a solution to route egress messages to appropriate logic

- Decode egress buffer
- Execute appropriate business logic

Using our Egress Receiver, we can use this to respond to websocket clients who made the request.

#### Obstacle Topics:
- Messaging Demultiplexing - How do we respond back to the websocket client?

### Task 6: Cluster Snapshotting

Your cluster should be able to take snapshots of the Orderbook state.

It should also be able to restore the taken snapshots from SnapshotImage back into in-memory state.

#### Obstacle Topics:
- Encoding/Decoding Java Data structures such as Hashmaps and Treesets

## Implementing a Web Gateway

The Vertx websocket server should function as follows:

- Receive websocket client requests following the JSON formats commented in the code implemenentation
- Send Ingress messages of these requests through our Aeron client to the cluster.
- Send JSON response back to the websocket client that made the request once response egress has been received.

## Bonus Topics:
- Back Pressure
- Idle Strategies
- JMH Benchmarking


Documentation:

- [Aeron Cookbook](https://aeroncookbook.com/aeron/overview/)
- [Aeron Github Wiki](https://github.com/real-logic/aeron/wiki)
- [Aeron Github Tutorial](https://github.com/real-logic/aeron/wiki/Cluster-Tutorial)

Reference Code:

- [Aeron Github Tutorial](https://github.com/real-logic/aeron/tree/1.40.0_tutorial_patch/aeron-samples/src/main/java/io/aeron/samples/cluster/tutorial)
- [Aeron.io Samples](https://github.com/AdaptiveConsulting/aeron-io-samples/tree/main)

```java --add-opens java.base/sun.nio.ch=ALL-UNNAMED -cp build/libs/aeron-oms-1.0-SNAPSHOT.jar io.aeron.cluster.ClusterTool aeronCluster/node0/cluster snapshot```
```java --add-opens java.base/sun.nio.ch=ALL-UNNAMED -cp build/libs/aeron-oms-1.0-SNAPSHOT.jar com.weareadaptive.cluster.ClusterMain 0 3 false```
```java --add-opens java.base/sun.nio.ch=ALL-UNNAMED -cp build/libs/aeron-oms-1.0-SNAPSHOT.jar com.weareadaptive.gateway.GatewayMain 3```
