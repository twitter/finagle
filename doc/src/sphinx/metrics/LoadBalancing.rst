All Balancers
<<<<<<<<<<<<<

**size**
  A gauge of the number of nodes being balanced across

**available**
  A gauge of the number of *available* nodes being balanced across

**load**
  A gauge of the total load over all nodes being balanced across

**meanweight**
  A gauge tracking the arithmetic mean of the weights of the endpoints
  being load-balanced across. Does not apply to
  `com.twitter.finagle.loadbalancer.HeapBalancer`.

**adds**
  A counter of the number of hosts added to the loadbalancer

**removes**
  A counter of the number of hosts removed from the loadbalancer

ApertureLoadBandBalancer
<<<<<<<<<<<<<<<<<<<<<<<<

**aperture**
  A gauge of the width of the window over which endpoints are
  load-balanced
