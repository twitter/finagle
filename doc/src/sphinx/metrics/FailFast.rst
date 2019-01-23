FailFastFactory
<<<<<<<<<<<<<<<

**marked_dead**
  A counter of how many times the host has been marked dead due to connection
  problems.

**marked_available**
  A counter of how many times the host has been marked available after Finagle
  reestablished a connection to a dead host.

**unhealthy_for_ms**
  A gauge of how long, in milliseconds, Finagle has been trying to reestablish
  a connection.

**unhealthy_num_tries**
  A gauge of the number of times the Factory has tried to reestablish a
  connection.

**is_marked_dead**
  A gauge of whether the host is marked as dead(1) or not(0).
