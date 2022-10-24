Tracing
<<<<<<<

**finagle/tracing/sampled**
  A counter for the number of times a positive sampling decision is made on an
  unsampled trace. A sampling decision is made using a client's configured ``Tracer``,
  using ``Tracer#sampleTrace``.

Aperture
<<<<<<<<

**finagle/aperture/coordinate**
  The process global coordinate for the process as sampled by
  the Aperture implementation.

**finagle/aperture/peerset_size**
  A gauge of the size of the services peerset.

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

Push Based Abstractions
<<<<<<<<<<<<<<<<<<<<<<<

**finagle/push/unhandled_exceptions**
  Family of counters for unhandled exceptions caught by the serial executor.

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

OffloadFilter
<<<<<<<<<<<<<

These metrics correspond to the state of the offload filter thread pool when configured. 

**finagle/offload_pool/pool_size**
  A gauge of the number of threads in the pool.

**finagle/offload_pool/active_tasks**
  A gauge of the number of tasks actively executing.

**finagle/offload_pool/completed_tasks**
  A gauge of the number of total tasks that have completed execution.

**finagle/offload_pool/not_offloaded_tasks**
  A counter of how many tasks weren't offloaded because the queue has grown over a proposed limit
  (set via a flag `com.twitter.finagle.offload.queueSize`). If a task can't be offloaded it is run
  the caller thread which is commonly a Netty IO worker.

**finagle/offload_pool/queue_depth**
  A Gauge of the number of tasks that are waiting to be executed.

**finagle/offload_pool/pending_tasks**
  A histogram reporting the number of pending tasks in the offload queue. For efficiency reasons,
  this stat is sampled each `com.twitter.finagle.offload.statsSampleInterval` interval. This stat is
  only enabled if `statsSampleInterval` is both positive and finite.

**finagle/offload_pool/delay_ms**
  A histogram reporting offloading delay - how long a task has been sitting in the offload queue
  before it gets executed. For efficiency reasons, this stat is sampled each
  `com.twitter.finagle.offload.statsSampleInterval` interval. This stat is  only enabled if
  `statsSampleInterval` is both positive and finite.

Timer
<<<<<

**finagle/timer/pending_tasks** `verbosity:debug`
  A stat of the number of pending tasks to run for the
  :src:`DefaultTimer <com/twitter/finagle/util/DefaultTimer.scala>`.

**finagle/timer/deviation_ms** `verbosity:debug`
  A stat of the deviation in milliseconds of tasks scheduled on the
  :src:`DefaultTimer <com/twitter/finagle/util/DefaultTimer.scala>` from their expected time.

**finagle/timer/slow**
  A counter of the number of tasks found to be executing for longer
  than 2 seconds.

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

**pending_io_events**
  A gauge of the number of pending IO events enqueued in all event loops servicing
  this client or server. If this metric climbs up, it indicates an overload scenario
  when IO threads are not being able to process the scheduled work (handling new
  requests and new connections). A very typical cause of these symptoms is either
  blocking or running a CPU intensive workloads on IO threads.

**worker_threads**
  A gauge for the size of the Netty worker pool. This will only
  reflect `EventLoopGroup`s constructed by Finagle and not those
  manually created by the application.

**pooling/allocations/huge** `verbosity:debug`
  A gauge of the total number of HUGE *direct allocations*
  (i.e., unpooled allocations that exceed the current chunk size).

**pooling/allocations/normal** `verbosity:debug`
  A gauge of the total number of NORMAL *direct allocations*
  (i.e., less than a current chunk size).

**pooling/allocations/small** `verbosity:debug`
  A gauge of the total number of SMALL *direct allocations*
  (i.e., less than a page size, 8192 bytes).

**pooling/allocations/tiny** `verbosity:debug`
  A gauge of the total number of TINY *direct allocations*
  (i.e., less than 512 bytes).

**pooling/deallocations/huge** `verbosity:debug`
  A gauge of the total number of HUGE *direct deallocations*
  (i.e., unpooled allocations that exceed the current chunk size).

**pooling/deallocations/normal** `verbosity:debug`
  A gauge of the total number of NORMAL *direct deallocations*
  (i.e., less than a chunk size).

**pooling/deallocations/small** `verbosity:debug`
  A gauge of the total number of SMALL *direct deallocations*
  (i.e., less than a page size, 8192 bytes).

**pooling/deallocations/tiny** `verbosity:debug`
  A gauge of the total number of TINY *direct deallocations*
  (i.e., less than 512 bytes).

**pooling/used*** `verbosity:debug`
  A gauge of the number of bytes used for *direct allocations* (this includes buffers in the
  thread-local caches).


**reference_leaks**
  A counter of detected reference leaks. See longer note on 
  `com.twitter.finagle.netty4.trackReferenceLeaks` for details.
