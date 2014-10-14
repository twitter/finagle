CachingPool
<<<<<<<<<<<

**pool_cached**
  a gauge of the number of connections cached in the idle Cache

WatermarkPool
<<<<<<<<<<<<<

**pool_waiters**
  a gauge of the number of clients waiting on connections

**pool_size**
  a gauge of the number of connections that are currently alive, either in use or not

**pool_num_waited**
  a counter of the number of times there were no connections immediately available and the client waited for a connection

**pool_num_too_many_waiters**
  a counter of the number of times there were no connections immediately available and there were already too many waiters

SingletonPool
<<<<<<<<<<<<<

**conn/fail**
  a counter of the number of times the connection could not be established and must be retried

**conn/dead**
  a counter of the number of times the connection succeeded once, but later died and must be retried
