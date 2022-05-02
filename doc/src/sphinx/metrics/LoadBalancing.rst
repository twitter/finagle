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

**busy_weight_classes**
  A counter of the number of times a weight class in the loadbalancer was found busy or
  closed.

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

**panicked**
  A counter of the number of times a balancer enters panic mode. The
  estimated portion of unhealthy nodes (status is not `Status.Open`)
  in the balancer exceeded the allowable threshold in 
  `com.twitter.finagle.loadbalancer.PanicMode`. When this occurs, a
  non-open node may be selected for that request.

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
  A gauge of the hash of the distributors serverset vector with range from
  [Int.MinValue, Int.MaxValue]. It is useful for identifying inconsistencies
  in the serverset observed by different instances of the same client since
  inconsistencies will result in very different values of vector_hash. This
  information is useful for identifying load banding issues when using the
  deterministic aperture load balancer which requires a consistent view of
  the backends to operate correctly.

**coordinate_updates**
  A counter of the number of times the Aperture implementation receives
  updates from the `ProcessCoordinate` process global.

**expired**
  A counter of the number of endpoints which have been closed because they
  have fallen out of the aperture window and become idle.
