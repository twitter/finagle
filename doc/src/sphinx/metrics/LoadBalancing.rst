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

**meanweight** `verbosity:debug`
  A gauge tracking the arithmetic mean of the weights of the endpoints
  being load-balanced across.

**num_weight_classes**
  The number of groups (or classes) of weights in the load balancer. Each class gets
  a fresh instance of the client's load balancer and receives traffic proportional
  to its weight.

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

Aperture Based Load Balancers
<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

**logical_aperture**
  A gauge of the width of the window over which endpoints are load-balanced.
  This is primarily an accounting mechanism and for a true representation of
  the number of endpoints the client is talking to see `physical_aperture`.

**physical_aperture**
  When using deterministic aperture (i.e. `useDeterministicOrdering` is set),
  the width of the window over which endpoints are load-balanced may be
  wider than the `logical_aperture` gauge. The `physical_aperture` represents
  this value.

**use_deterministic_ordering**
  1 if the Aperture implementation uses deterministic ordering
  0, otherwise.

**vector_hash**
  A gauge of the hash of the distributors serverset vector.

**coordinate_updates**
  A counter of the number of times the Aperture implementation receives
  updates from the `ProcessCoordinate` process global.

**rebuild_no_coordinate**
  A counter which tracks the number of rebuilds without a `coordinate` set
  when `use_deterministic_ordering` is 1.

**expired**
  A counter of the number of endpoints which have been closed because they
  have fallen out of the aperture window and become idle.
