**<label>/rollback_latency_ms**
  The latency of the automatic rollback issued when a connection
  is put back into the pool.

**<label>/cursor/time_per_stream_ms**
  The latency from start to finish of a cursor stream.

**<label>/cursor/time_per_fetch_ms**
  The latency of individual cursor fetches.

**<label>/cursor/time_between_fetch_ms**
  The latency between individual cursor fetches.

**<label>/cursor/opened**
  A counter of the number of opened cursors.

**<label>/cursor/closed**
  A counter of the number of closed cursors.

**<label>/pstmt-cache/calls**
  A counter of the number of requested prepared statements.

**<label>/pstmt-cache/misses**
  A counter of the number of times when a prepared statement was not the cache.

**<label>/pstmt-cache/evicted_size**
  A counter of the number of times prepared statements were evicted from the cache.
