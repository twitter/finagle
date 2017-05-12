FuturePool
<<<<<<<<<<

These metrics correspond to the state of ``FuturePool.unboundedPool`` and
``FuturePool.interruptibleUnboundedPool``. Only one set of metrics is
exported as they share their underlying "thread pool".

**finagle/future_pool/pool_size**
  A gauge of the number of threads in the pool.

**finagle/future_pool/active_tasks**
  A gauge of the number of tasks actively executing.

**finagle/future_pool/completed_tasks**
  A gauge of the number of total tasks that have completed execution.

Scheduler
<<<<<<<<<

**scheduler/dispatches**
  A gauge of the number of dispatches performed by the
  ``com.twitter.concurrent.Scheduler``.

**scheduler/blocking_ms**
  A gauge of how much time, in milliseconds, the ``com.twitter.concurrent.Scheduler``
  is spending doing blocking operations on threads that have opted into tracking.
  Of the built-in ``Schedulers``, this is only enabled for the
  ``com.twitter.concurrent.LocalScheduler`` which is the default ``Scheduler``
  implementation. Note that this does not include time spent doing blocking code
  outside of ``com.twitter.util.Await.result``/``Await.ready``. For example,
  ``Future(someSlowSynchronousIO)`` would not be accounted for in this metric.

Timer
<<<<<

**finagle/timer/pending_tasks**
  A stat of the number of pending tasks to run for
  :src:`HashedWheelTimer.Default <com/twitter/finagle/util/HashedWheelTimer.scala>`.

**finagle/timer/deviation_ms**
  A stat of the deviation in milliseconds of tasks scheduled on
  :src:`HashedWheelTimer.Default <com/twitter/finagle/util/HashedWheelTimer.scala>`
  from their expected time.

ClientRegistry
<<<<<<<<<<<<<<

**finagle/clientregistry/size**
  A gauge of the current number of clients registered in the
  :src:`ClientRegistry <com/twitter/finagle/client/ClientRegistry.scala>`.

Name Resolution
<<<<<<<<<<<<<<<

**inet/dns/queue_size**
  A gauge of the current number of DNS resolutions waiting for
  lookup in :src:`InetResolver <com/twitter/finagle/Resolver.scala>`.

**inet/dns/dns_lookups**
  A counter of the number of DNS lookups attempted by :src:`InetResolver
  <com/twitter/finagle/Resolver.scala>`.

**inet/dns/dns_lookup_failures**
  A counter of the number of DNS lookups attempted by :src:`InetResolver
  <com/twitter/finagle/Resolver.scala>` and failed.

**inet/dns/lookup_ms**
  A histogram of the latency, in milliseconds, of the time to lookup
  every host (successfully or not) in a ``com.twitter.finagle.Addr``.

**inet/dns/successes**
  A counter of the number of ``com.twitter.finagle.Addr`` s with
  at least one resolved host.

**inet/dns/failures**
  A counter of the number of ``com.twitter.finagle.Addr`` s with
  no resolved hosts.

**inet/dns/cache/size**
  A gauge of the approximate number of cached DNS resolutions in
  :src:`FixedInetResolver <com/twitter/finagle/Resolver.scala>`.

**inet/dns/cache/evicts**
  A gauge of the number of times a cached DNS resolution has been
  evicted from :src:`FixedInetResolver
  <com/twitter/finagle/Resolver.scala>`.

**inet/dns/cache/hit_rate**
  A gauge of the ratio of DNS lookups which were already cached by
  :src:`FixedInetResolver <com/twitter/finagle/Resolver.scala>`


Netty 4
<<<<<<<

These metrics are exported from Finagle's underlying transport
implementation, the Netty 4 library and available under `finagle/netty4`
on any instance running Finagle with Netty 4.

NOTE: All pooling metrics are only exported when pooling is enabled
      (default: disabled) and only account for direct memory.

**pooling/allocations/huge**
  A gauge (a counter) of total number of HUGE *direct allocations*
  (i.e., unpooled allocations that exceed the current chunk size).

**pooling/allocations/normal**
  A gauge (a counter) of total number of NORMAL *direct allocations*
  (i.e., less than a current chunk size).

**pooling/allocations/small**
  A gauge (a counter) of total number of SMALL *direct allocations*
  (i.e., less than a page size, 8192 bytes).

**pooling/allocations/tiny**
  A gauge (a counter) of total number of TINY *direct allocations*
  (i.e., less than 512 bytes).

**pooling/deallocations/huge**
  A gauge (a counter) of total number of HUGE *direct deallocations*
  (i.e., unpooled allocations that exceed the current chunk size).

**pooling/deallocations/normal**
  A gauge (a counter) of total number of NORMAL *direct deallocations*
  (i.e., less than a chunk size).

**pooling/deallocations/small**
  A gauge (a counter) of total number of SMALL *direct deallocations*
  (i.e., less than a page size, 8192 bytes).

**pooling/deallocations/tiny**
  A gauge (a counter) of total number of TINY *direct deallocations*
  (i.e., less than 512 bytes).

**reference_leaks**
  A counter of detected reference leaks. See longer note on 
  `com.twitter.finagle.netty4.trackReferenceLeaks` for details.
