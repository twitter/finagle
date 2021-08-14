.. _partitionawareclient:

ThriftMux Partition Aware Client
================================

.. note:: This set of APIs are currently experimental.

This page demonstrates how to enable partition aware routing for ThriftMux/Thrift Client.

Terms
-----

.. glossary::

Partition
  A partition represents a logical entity that processes data.
  It could be a single physical instance or a set of physical instances, each instance
  in a partition can be treated as equivalent over the data that the partition is responsible
  for. Partitions can overlap, instances can belong to different partitions.

Shard / Instance
  A physical instance.

Enable Partition Awareness
--------------------------

Thrift/ThriftMux Client
<<<<<<<<<<<<<<<<<<<<<<<

The set of APIs to configure a client with partitioning is at :finagle-thrift-src:`PartitioningParams
<com/twitter/finagle/thrift/exp/partitioning/PartitioningParams.scala>`.

- `.strategy(partitioningStrategy: PartitioningStrategy)` configures the client with a
  PartitioningStrategy; it could be either a CustomPartitioningStrategy or a HashingPartitioningStrategy.

- Params only for HashingPartitioningStrategy

  - `.ejectFailedHost(eject: Boolean)` partitions are placed on a
    hash ring, when failure accrual marks partitions as unhealthy,
    this param is for deciding whether to eject the failing
    partitions from the hash ring. By default, this is off.

  - `.keyHasher(hasher:KeyHasher)`, defines the hash function to use
    for partitioned clients when mapping keys to partitions.

  - `.numReps(reps: Int)`, duplicate each node across the hash ring according to reps,
    this will adjust the distribution of partition nodes on the hash ring, higher value
    means more even. The default value is 160.

Use the following code snippet to enable partition awareness for a Finagle client of a hashing
strategy and custom strategy respectively:

.. code-block:: scala
   :linenos:

   import com.twitter.finagle.ThriftMux
   import com.twitter.finagle.thrift.exp.partitioning.{ClientCustomStrategy, ClientHashingStrategy}

   val hashingPartitioningStrategy: ClientHashingStrategy = ???
   val clientWithHashing = ThriftMux.client
     .withPartitioning.strategy(hashingPartitioningStrategy)
     .withPartitioning.ejectFailedHost(false)

   val customPartitioningStrategy: ClientCustomStrategy = ???
   val clientWithHashing = ThriftMux.client
     .withPartitioning.strategy(customPartitioningStrategy)

ThriftMux MethodBuilder
<<<<<<<<<<<<<<<<<<<<<<<

MethodBuilder is constructed at a higher level than Finagle 6 APIs and is designed for
customizing each endpoint. Partitioning configuration can be specified for each endpoint.
The hashing Params mentioned above are still at the Finagle client stack layer as they
have pretty universal defaults and should be rarely configured for each endpoint.

To apply a partitioning strategy to a MethodBuilder endpoint:

`.withPartitioningStrategy(partitioningStrategy: PartitioningStrategy)` , this can
be used to apply different strategies for different endpoints built from one MethodBuilder.

The main difference between MethodBuilder endpoints vs Finagle client stack is that the former
are customized for each endpoint, so that partitioning strategy applied only needs to take care
of one endpoint while the latter has to take all endpoints into consideration by using
PartialFunction. Below we demonstrate use cases based on Finagle client stack and the
MethodBuilder examples can be found in Appendix.

Which Partitioning Strategy to choose?
--------------------------------------

To configure a ThriftMux Partition Aware Client, Finagle provides two abstractions,
CustomPartitioningStrategy and HashingPartitioningStrategy, for applications to choose.

- **HashingPartitioningStrategy** is equipped with a consistent hashing algorithm
  underlying to distribute partitions on a hash ring. It applies hashing functions to
  each request and routes the request to the destination node. HashingPartitioningStrategy
  can free service operators to manage the partitioning topology. Besides, unlike custom
  partitioning strategy, this supports scaling natively by minimizing the load change
  when scaling up or down. Although the hashing strategy has these mechanisms built in,
  they are not sensitive to your workload and may or may not work for your topology perfectly.

- **CustomPartitioningStrategy** provides more flexibility to define the partitioning topology,
  since the client configuration takes full control of the requests distribution.
  This expects a careful implementor to handle hot shards properly by anticipating and
  coordinating traffic flows. An example use case of custom partitioning is the key range
  strategy, which is dividing the entire keyset into continuous ranges and assigning each
  range to a partition.

This set of partitioning APIs is experimental and the `CustomPartitioningStrategy` comes with
dynamic resharding support. They provide plenty of flexibility for services with different
scenarios, aside from choosing between custom and hashing strategies, whether the client is doing
`scatter/gather (fan-out) <https://en.wikipedia.org/wiki/Vectored_I/O>`_, and if the partitioning
service needs to implement dynamic resharding, we will talk about setups for each scenario listed
below.

How to implement a hashing strategy?
<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

Example thrift service: deliveryService.thrift

.. code-block:: thrift
   :caption: deliveryService.thrift
   :name: deliveryService-thrift
   :linenos:

   namespace java com.twitter.delivery.thriftjava
   #@namespace scala com.twitter.delivery.thriftscala

   exception AException {
    1: i32 errorCode
   }

   service DeliveryService {
     // non-fanout message
     Box getBox(1: AddrInfo addrInfo, 2: i8 passcode) throws (
       1: AException ex
     )
     // fan-out message, easy to merge
     list<Box> getBoxes(1: list<AddrInfo> listAddrInfo, 2: i8 passcode) throws (
       1: AException ex
     )
   }

   struct AddrInfo {
     1: string name;
     2: i32 zipCode;
   }

   struct Box {
     1: AddrInfo addrInfo;
     2: string item;
   }

.. _mergeable:

Note that this example has the `getBoxes` endpoint to demonstrate how to do messaging
fan-out. Fan-out requests and responses should be in mergeable formats, which means
they can be split and merge programmably, such as an array type variable. Users need to
implement the request splitting and request/response merging functions, see fan-out
sections for details.

Clients don’t fan-out
"""""""""""""""""""""

Users need to implement a concrete `getHashingKeyAndRequest` method. It is a PartialFunction
that takes a Thrift request and returns a Map of hash keys to Thrift requests. The non-fanout
case is the simplified version of mapping requests, and it always returns a map of one
user-specified hash key and the Thrift request.

Turning the example into code:

.. code-block:: scala
  :caption: Configure the request routing function
  :linenos:

  import com.twitter.delivery.thriftscala.Box
  import com.twitter.delivery.thriftscala.DeliveryService._
  import com.twitter.finagle.thrift.exp.partitioning.ClientHashingStrategy

  val getHashingKeyAndRequest: ClientHashingStrategy.ToPartitionedMap = {
    // specify the AddrInfo.name as the hash key
    case getBox: GetBox.Args => Map(getBox.addrInfo.name -> getBox)
  }

  val hashingPartitioningStrategy = new ClientHashingStrategy(getHashingKeyAndRequest)

The PartialFunction allows a single partitioning strategy to work differently for various
Thrift endpoints (methods) of a service. The unspecified methods will fall into the built-in
`defaultHashingKeyAndRequest`. This allows a partition aware client to serve a part of endpoints
of a service. Un-specified endpoints should not be called from this client, if called,
the client throws `NoPartitioningKeys` exceptions. If the client is using MethodBuilder,
which is a per-endpoint setup, `getHashingKeyAndRequest` is a function instead of a partial
function.

Clients do messaging fan-out
""""""""""""""""""""""""""""

Expanding from the example above, fan-out requests need to implement the `getHashingKeyAndRequest`
with a map of hash keys to sub-requests. It needs to reconstruct the split Thrift requests:

.. code-block:: scala
  :caption: Configure the request routing function
  :linenos:

  import com.twitter.delivery.thriftscala.DeliveryService._
  import com.twitter.finagle.thrift.exp.partitioning.ClientHashingStrategy

  val getHashingKeyAndRequest: ClientHashingStrategy.ToPartitionedMap = {
    case getBoxes: GetBoxes.Args =>
      getBoxes.listAddrInfo
        .groupBy(_.name).map {
          case (hashingKey, subListAddrInfo) =>
            hashingKey -> GetBoxes.Args(subListAddrInfo, getBoxes.passcode)
        }
  }
  val hashingPartitioningStrategy = new ClientHashingStrategy(getHashingKeyAndRequest)

Because of the nature of consistent hashing, multiple hash keys can fall on the same partition.
Users need to instruct the Finagle partitioning layer to merge requests properly.
A `RequestMerger` helper function is for this purpose, which takes a sequence of Thrift
requests and returns a single request, the Thrift requests should be
in mergeable_ formats.

.. code-block:: scala
  :caption: Set the request merging function
  :linenos:

  import com.twitter.finagle.thrift.exp.partitioning.PartitioningStrategy.RequestMerger

  val getBoxesReqMerger: RequestMerger[GetBoxes.Args] = listGetBoxes =>
    GetBoxes.Args(listGetBoxes.map(_.listAddrInfo).flatten, listGetBoxes.head.passcode)

Fan-out messages mean the client receives responses from a set of partitions.
A `ResponseMerger` also needs to be defined to handle batched successes and failures.
`ResponseMerger` takes a sequence of success response and a sequence of failures then returns a
`Try[ResponseType]``:

.. code-block:: scala
  :caption: Set the response merging function
  :linenos:

  import com.twitter.finagle.thrift.exp.partitioning.PartitioningStrategy.ResponseMerger
  import com.twitter.util.{Return, Throw}

  val getBoxesRepMerger: ResponseMerger[Seq[Box]] = (successes, failures) =>
    if (successes.nonEmpty) Return(successes.flatten)
    else Throw(failures.head)


The last step is to register the defined `RequestMerger` and `ResponseMerger` with
the `ThriftMethod` within the partitioning strategy by adding mergers
through `requestMergerRegistry` and `responseMergerRegistry`.
Note that multiple `ThriftMethod` s can be cascaded to the registries.

.. code-block:: scala
  :caption: Register the Merging functions
  :linenos:

  hashingPartitioningStrategy.requestMergerRegistry.add(GetBoxes, getBoxesReqMerger)
  hashingPartitioningStrategy.responseMergerRegistry.add(GetBoxes, getBoxesRepMerger)

How to implement a custom strategy?
<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

Custom strategy shares the same matrix of fan-out/non-fan-out as hashing strategy.
Furthermore, it provides more flexibility to manage the backend partitioning topology.
It lets group many shards to one logical partition and one shard to belong to multiple partitions.
Custom partition also supports dynamic re-sharding by observing a user-provided state.
There are three sets of APIs to use based on different resharding needs.

-  **noResharding**: no dynamic resharding, the backend partitioning
   topology stays static.

-  **resharding**: lets the client be aware of the backend dynamic
   resharding by providing the fully described state of resharding. The
   partitioning schema needs to be configured to react to each state,
   and it needs to be a `pure
   function <https://en.wikipedia.org/wiki/Pure_function>`__. When the
   state gets successfully updated, the partitioning strategy will move
   to the new schema.

-  **clusterResharding**: a semi-implemented version of resharding, which
   is appropriate when only cluster information needs to be observed in
   order to reshard. For example, if you want to be able to add or
   remove capacity safely.

We will show examples of implementing a noResharding strategy, as it shares the fundamental
ideas with the others. Taking the same example deliveryService-thrift_:

.. note::
  For resharding strategy, check :finagle-thrift-src:`API docs
  <com/twitter/finagle/thrift/exp/partitioning/PartitioningStrategy.scala#L445>`
  and :finagle-thrift-test:`test example
  <com/twitter/finagle/thrift/exp/partitioning/PartitionAwareClientEndtoEndTest.scala#L359>`.

  For clusterResharding strategy, check :finagle-thrift-src:`API docs
  <com/twitter/finagle/thrift/exp/partitioning/PartitioningStrategy.scala#L369>`
  and :finagle-thrift-test:`test example
  <com/twitter/finagle/thrift/exp/partitioning/PartitionAwareClientEndtoEndTest.scala#L422>`.

Clients don’t fan-out
"""""""""""""""""""""

Users need to implement a concrete `getPartitionIdAndRequest` method. It is a PartialFunction
that takes a Thrift request and returns a Future of Map(partition Id -> Thrift request).
The non-fanout case is the simplified version of mapping requests, and it always returns a
map of one partition Id and the Thrift request. The partition Id is an integer, if the
service’s address metadata is backed by ZooKeeper, partition Id is the `shardId` announced
by ZooKeeper, and if Aurora is used as the service scheduler, it is the same as the Aurora job
Id. The `getPartitionIdAndRequest` utilizes `Future` so that partitioning data can be
fetched from an RPC call.

Turning the example into code:

.. code-block:: scala
  :caption: Configure the request routing function
  :linenos:

  import com.twitter.delivery.thriftscala.AddrInfo
  import com.twitter.delivery.thriftscala.DeliveryService._
  import com.twitter.finagle.thrift.exp.partitioning.ClientCustomStrategy
  import com.twitter.util.Future

  def lookUp(addrInfo: AddrInfo): Int = {
    addrInfo.name match {
      case "name1" | "name2" => 0 // partition 0
      case "name3" => 1 // partition 1
    }
  }

  val getPartitionIdAndRequest: ClientCustomStrategy.ToPartitionedMap = {
    case getBox: GetBox.Args => Future.value(Map(lookUp(getBox.addrInfo) -> getBox))
  }

  val customPartitioningStrategy =
    ClientCustomStrategy.noResharding(getPartitionIdAndRequest)

The PartialFunction allows the partitioning strategy to arrange multiple Thrift request
types of one Thrift service for different endpoints(methods) of a service.
The undefined request type will fall into the built-in `defaultPartitionIdAndRequest`.
This allows a partition aware client to serve a part of endpoints of a service.
Un-specified endpoints should not be called from this client, otherwise,
throw `PartitioningStrategyException` exceptions. If the client is using MethodBuilder,
which is per-endpoint, `getPartitionIdAndRequest` is a function.

Besides, CustomPartitioningStrategy supports applications to use logical partitions,
which groups a set of shards to one partition, and one shard performs in multiple partitions.

.. code-block:: scala
  :caption: Set up the logical partition mapping
  :linenos:

  // group instances to logical partition
  // partition0 (instance 0 - 9), partition1(instance 0 - 19) partition3(instance 20 - 29)
  val getLogicalPartition: Int => Seq[Int] = {
    case a if Range(0, 10).contains(a) => Seq(0, 1)
    case b if Range(10, 20).contains(b) => Seq(1)
    case c if Range(20, 30).contains(c) => Seq(2)
    case _ => throw new Exception(“out of index”)
  }

  val customPartitioningStrategy = ClientCustomStrategy.noResharding(getPartitionIdAndRequest, getLogicalPartition)

Clients do messaging fan-out
""""""""""""""""""""""""""""

Expanding from non fan-out setup above, fan-out requests need to implement the
`getPartitionIdAndRequest` with a Future Map of partition Ids to sub-requests. It needs to
reconstruct the split Thrift requests:

.. code-block:: scala
  :caption: Configure the request routing function
  :linenos:

  val getPartitionIdAndRequest: ClientCustomStrategy.ToPartitionedMap = {
    case getBoxes: GetBoxes.Args =>
      Future.value(getBoxes.listAddrInfo.groupBy(lookUp).map {
        case (partitionId, listAddrInfo) =>
          partitionId -> GetBoxes.Args(listAddrInfo, getBoxes.passcode)
      })
  }

  val customPartitioningStrategy = ClientCustomStrategy.noResharding(getPartitionIdAndRequest, getLogicalPartition)

Fan-out messages means the client receives responses from a set of partitions.
A `ResponseMerger` also needs to be defined to handle successes and failures separately.
`ResponseMerger` takes a sequence of success response and a sequence of failures then returns a
`Try[ResponseType]`.

The last step is to register the defined `ResponseMerger` with the `ThriftMethod` within the
partitioning strategy by adding mergers through `responseMergerRegistry`. Note that multiple
`ThriftMethod` s can be cascaded.

.. code-block:: scala
  :caption: Set the Response mergers and register the Merging functions
  :linenos:

  import com.twitter.finagle.thrift.exp.partitioning.PartitioningStrategy.ResponseMerger

  val getBoxesRepMerger: ResponseMerger[Seq[Box]] = (successes, failures) =>
    if (successes.nonEmpty) Return(successes.flatten)
    else Throw(failures.head)
  customPartitioningStrategy.responseMergerRegistry.add(GetBoxes, getBoxesRepMerger)


Metrics
-------

.. include:: metrics/Partitioning.rst

Appendix
--------

Example of using MethodBuilder custom partitioning strategy:

.. code-block:: scala

  def lookUp(addrInfo: AddrInfo): Int = {
    addrInfo.name match {
      case "name1" | "name2" => 0 // partition 0
      case "name3" => 1 // partition 1
    }
  }

  // group instances to logical partition
  // partition0 (instance 0 - 9), partition1(instance 0 - 19) partition3(instance 20 - 29)
  val getLogicalPartition: Int => Seq[Int] = {
    case a if Range(0, 10).contains(a) => Seq(0, 1)
    case b if Range(10, 20).contains(b) => Seq(1)
    case c if Range(20, 30).contains(c) => Seq(2)
    case _ => throw new Exception("out of index")
  }

  // response merger functions
  val getBoxesRepMerger: ResponseMerger[Seq[Box]] = (successes, failures) =>
    if (successes.nonEmpty) Return(successes.flatten)
    else Throw(failures.head)

  val methodBuilderStrategy1 = new MethodBuilderCustomStrategy[GetBoxes.Args, Seq[Box]](
    { getBoxes: GetBoxes.Args =>
      val partitionIdAndRequest: Map[Int, GetBoxes.Args] =
        getBoxes.listAddrInfo.groupBy(lookUp).map {
          case (partitionId, listAddrInfo) =>
            partitionId -> GetBoxes.Args(listAddrInfo, getBoxes.passcode)
        }
      Future.value(partitionIdAndRequest)
    },
    getLogicalPartition,
    Some(getBoxesRepMerger)
  )

  val methodBuilderStrategy2 = new MethodBuilderCustomStrategy[GetBox.Args, Box](
    { getBox: GetBox.Args =>
      Future.value(Map(lookUp(getBox.addrInfo) -> getBox))
    },
    getLogicalPartition
  )

  val builder = ThriftMux.client.methodBuilder(???)

  val getBoxesEndpoint = builder
    .withPartitioningStrategy(methodBuilderStrategy1)
    .servicePerEndpoint[DeliveryService.ServicePerEndpoint](methodName = "getBoxes")
    .getBoxes

  val getBoxEndpoint = builder
    .withPartitioningStrategy(methodBuilderStrategy2)
    .servicePerEndpoint[DeliveryService.ServicePerEndpoint](methodName = "getBox")
    .getBox
