MinimumSetCluster
<<<<<<<<<<<<<<<<<

**censored_add**
  a counter of the number of times there is an attempt to add a server that is already in the
  minimum set to the cluster

**censored_rem**
  a counter of the number of times there is an attempt to remove a server that is guaranteed to be
  in the minimum set to the cluster

**missing**
  a gauge of the number of servers in the bag-difference between the supplementary cluster and the minimum set.
  this should be the number of servers that would be missing if we only used the supplementary cluster

**additional**
  a gauge of the number of servers in the bag-difference between the minimum set and the supplementary cluster.
  this should be the number of servers that would be missing if we only used the minimum set

HeapBalancer
<<<<<<<<<<<<

**size**
  a gauge of the number of nodes in the heap

**adds**
  a counter of the number of adds to the heap

**removes**
  a counter of the number of removes to the heap

**available**
  a gauge of the number of available nodes in the heap

**load**
  a gauge of the total load over all nodes in the heap
