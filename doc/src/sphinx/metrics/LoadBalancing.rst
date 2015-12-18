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
  :src:`HeapBalancer <com/twitter/finagle/loadbalancer/HeapBalancer.scala>`.

**adds**
  A counter of the number of hosts added to the loadbalancer.

**removes**
  A counter of the number of hosts removed from the loadbalancer.

**max_effort_exhausted**
  A counter of the number of times a balancer failed to find a node that was
  ``Status.Open`` within ``com.twitter.finagle.loadbalancer.Balancer.maxEffort``
  attempts. When this occurs, a non-open node may be selected for that
  request.

ApertureLoadBandBalancer
<<<<<<<<<<<<<<<<<<<<<<<<

**aperture**
  A gauge of the width of the window over which endpoints are
  load-balanced.
