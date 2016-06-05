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
  :src:`HashedWheelTimer.Default <com/twitter/finagle/client/ClientRegistry.scala>`.

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
