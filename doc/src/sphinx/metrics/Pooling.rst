CachingPool
<<<<<<<<<<<

**pool_cached**
  A gauge of the number of connections cached.

WatermarkPool
<<<<<<<<<<<<<

**pool_waiters**
  A gauge of the number of clients waiting on connections.

**pool_size**
  A gauge of the number of connections that are currently alive, either in use
  or not.

**pool_num_waited**
  A counter of the number of times there were no connections immediately
  available and the client waited for a connection.

**pool_num_too_many_waiters**
  A counter of the number of times there were no connections immediately
  available and there were already too many waiters.

SingletonPool
<<<<<<<<<<<<<

**conn/fail**
  A counter of the number of times the connection could not be established and
  must be retried.

**conn/dead**
  A counter of the number of times the connection succeeded once, but later
  died and must be retried.
