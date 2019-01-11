Deterministic Aperture
----------------------

Deterministic aperture is a refinement of random aperture that has better load
distribution and connection count properties. Like random aperture, deterministic
aperture reduces the overhead of clients for both clients and the servers they talk to
by only talking to a subset of service instances, unlike P2C which lets the client pick
from any replica in the serverset. However, unlike the classic aperture implementation,
it does so using a deterministic algorithm which distributes load evenly across service
instances, reducing the phenomena of load banding which is inherent to the statistical
approach employed by random aperture. This facilitates a further reduction in necessary
connections and manual configuration. Deterministic Aperture is the default load balancer
at Twitter.


*Service B RPS during a rolling restart of service A which changed the load balancer from random
aperture to determinstic aperture*

.. figure:: /_static/t-to-g-daperturerolloutrps.png
   :align: center

   The variance in requests per second (RPS) is significantly diminished after the service A deploy
   due to the deterministic load distribution.

*Connections from service A to service B during a rolling restart of service A which changed the
load balancer from random aperture to determinstic aperture*

.. figure:: /_static/t-to-g-daperturerolloutconnections.png
   :align: center

   The total number of connections from A to B was significantly reduced.

Deterministic Subsetting
^^^^^^^^^^^^^^^^^^^^^^^^

Deterministic aperture combines information about both the backend cluster as well as the
cluster information for the service at hand and partitions connections to backends in a
way that favors equal load and minimal connections (with a lower bound of 12 to satisfy
common concurrency and redundancy needs). It does this by letting each member of a cluster
compute a subset of weighted backends to talk to based on information about each cluster.
This can be visually represented as a pair of overlapping rings: The outer ring, or "peer ring",
represents the cluster of peers (Service A) which are acting in concert to offer load to the
"destination ring" which represents the cluster of backends (Service B).

.. figure:: /_static/daperture-ringdiagram1.png
   :align: center

   Each instance of Service A determines which "slice" of the backend ring to talk to and
   directs its traffic to those specific instances.

.. figure:: /_static/daperture-ringdiagram2.png
   :align: center

   Since each client instance is doing the same thing, except picking the slice that belongs
   to them, we can evenly load the backend cluster (Service B).
   Note that this requires the load balancer to respect the continuous ring representation
   since peer slices may cover fractional portions of the destination ring.

Traffic Patterns
^^^^^^^^^^^^^^^^

The deterministic aperture model, and subsetting models in general, works well when the
traffic pattern between service A and B is "smooth" meaning that for each instance of
Service A the request pattern doesn't consists of significant bursts with respect to the
average request rate and that requests are roughly equal cost, both in resource usage of
the backend and in terms of latency. If the traffic pattern is characterized by
large bursts of traffic, say invalidation of a cache based on a timer, then small chunks
of the ring will end up extremely loaded for the duration of the burst while other slices
may be essentially idle. Under those conditions the P2C load balancer may be more
appropriate so the entire backend cluster can absorb the large bursts coming from instances
of Service A.

Peer Information
^^^^^^^^^^^^^^^^

In order for deterministic aperture to be deterministic it needs to know some additional
information. This is important for computing the "slice" of backends that the load balancer
will utilize. To know how large of slice to use the client needs information about both the
backends, or the serverset, and it needs to know some information about its peers, also
known as the peerset. The peerset is the only new information needed since knowing about
the backends is a requirement for any client side load balancer. In regard to the peerset
we need two different pieces of information:

- How many replicas are there in the peerset. This is used to compute how large each clients
  slice of the backends will be.

- What is the position of this instance in the peerset. This is required to compute which
  slice that this client should take.

This information can be configured in the `ProcessCoordinate <https://github.com/twitter/finagle/blob/develop/finagle-core/src/main/scala/com/twitter/finagle/loadbalancer/aperture/ProcessCoordinate.scala>`_
object by setting the processes instance number and the total number of instances. The current
implementation requires that all instances in the cluster have unique and contiguous instance id's
starting at 0.

Key Metrics
^^^^^^^^^^^

Success rates and exceptions should not be affected by the new load balancer. Other
important metrics to consider are request latency and connection count.

::

  $clientName/request_latency_ms.p*

  $clientName/connections

  $clientName/loadbalancer/vector_hash

  finagle/aperture/coordinate
