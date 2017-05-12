All Balancers
<<<<<<<<<<<<<

**size**
  A gauge of the number of nodes being balanced across.

**available**
  A gauge of the number of *available* nodes as seen by the load balancer.
  These nodes are ready to receive traffic.

**busy**
  A gauge of the number of *busy* nodes as seen by the load balancer.
  These nodes are current unavailable for service.

**closed**
  A gauge of the number of *closed* nodes as seen by the load balancer.
  These nodes will never be available for service.

**load**
  A gauge of the total load over all nodes being balanced across.

**meanweight**
  A gauge tracking the arithmetic mean of the weights of the endpoints
  being load-balanced across. Does not apply to
  :src:`HeapLeastLoaded <com/twitter/finagle/loadbalancer/heap/HeapLeastLoaded.scala>`.

**adds**
  A counter of the number of hosts added to the loadbalancer.

**removes**
  A counter of the number of hosts removed from the loadbalancer.

**rebuilds**
   A counter of the number of times the loadbalancer rebuilds its state
   (triggered by either an underlying namer or failing nodes).

**updates**
   A counter of the number of times the underlying namer triggers
   the loadbalancer to rebuild its state (e.g., because the server set
   has changed). Note that these kind of events are usually collapsed
   so the actual number of ``rebuilds`` is usually less than the number
   of ``updates``.

**max_effort_exhausted**
  A counter of the number of times a balancer failed to find a node that was
  ``Status.Open`` within ``com.twitter.finagle.loadbalancer.Balancer.maxEffort``
  attempts. When this occurs, a non-open node may be selected for that
  request.

**algorithm/{type}**
  A gauge exported with the name of the algorithm used for load balancing.

ApertureLoadBandBalancer
<<<<<<<<<<<<<<<<<<<<<<<<

**aperture**
  A gauge of the width of the window over which endpoints are
  load-balanced.

**coordinate**
  The process global coordinate for the process as sampled by
  the Aperture implementation.

**use_deterministic_ordering**
  1 if the Apeture implementation uses deterministic ordering
  0, otherwise.

**coordinate_updates**
  A counter of the number of times the Aperture implementation receives
  updates from the `DeterministicOrdering` process global.
