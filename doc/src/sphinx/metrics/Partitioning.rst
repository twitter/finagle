Partitioning metrics are under **clnt/<server_label>/partitioner/** scope given insights
into how client stack manages partition nodes.

HashingPartitioningStrategy
<<<<<<<<<<<<<<<<<<<<<<<<<<<

redistributes
  A counter of the times the nodes on the hash ring have been redistributed.

joins
  A counter of the times a new node joins the hash ring, indicating a new partition
  joins the cluster.

leaves
  A counter of the times a node leaves the hash ring, indicating service discovery
  detects a node leave.

ejections
  A counter of the times an unhealthy node marked by ConsistentHashingFailureAccrual
  has been removed from the hash ring. *leaves* and *joins* indicate service discovery updates,
  while *ejections* and *revivals* indicate the code health status.

revivals
  A counter of the times an ejected node has been marked as live on the hash ring. *leaves*
  and *joins* indicate service discovery updates, while *ejections* and *revivals* indicate
  node health status.

live_nodes
  A gauge of the current total number of healthy partitions.

dead_nodes
  A gauge of the current total number of unhealthy partitions marked by
  ConsistentHashingFailureAccrual.

CustomPartitioningStrategy (ThriftMux)
<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

nodes
  A gauge of the current total number of logical partitions.
