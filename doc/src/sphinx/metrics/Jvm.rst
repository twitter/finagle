A wide variety of metrics are exported which provide insight into the JVM's
runtime. These include garbage collection, allocations, threads, uptime,
and more.

If you are using a Hotspot VM, additional metrics are exported that
may be useful. This includes safe point time (`jvm/safepoint`),
metaspace usage (`jvm/mem/metaspace`), allocation rates (`jvm/mem/allocations`),
time running application code since start (`jvm/application_time_millis`),
and tenuring threshold (`jvm/tenuring_threshold`).

**jvm/mem/allocations/eden/bytes**
  A gauge of the number of bytes allocated into the eden. This is a particularly
  relevant metric for service developers. The vast majority of allocations are
  done into the eden space, so this metric can be used to calculate the allocations
  per request which in turn can be used to validate code changes
  don't increase garbage collection pressure on the hot path.

**jvm/gc/msec**
  A gauge of the total elapsed time doing collections, in milliseconds.

**jvm/gc/cycles**
  A gauge of the number of the total number of collections that have occurred.

**jvm/gc/eden/pause_msec**
  A stat of the durations, in millseconds, of the eden collection pauses.

**jvm/gc/{gc_pool_name}/msec**
  A gauge for the named gc pool of the total elapsed time garbage collection pool
  doing collections, in milliseconds. Names are subject to change.

**jvm/gc/{gc_pool_name}/cycles**
  A gauge for the named gc pool of the total number of collections that have occurred.
  Names are subject to change.

**jvm/thread/count**
  A gauge of the number of live threads including both daemon and non-daemon threads.

**jvm/thread/daemon_count**
  A gauge of the number of live daemon threads.

**jvm/thread/peak_count**
  A gauge of the peak live thread count since the Java virtual machine started or peak was reset.

**jvm/uptime**
  A gauge of the uptime of the Java virtual machine in milliseconds.

**jvm/spec_version**
  A gauge of the running Java virtual machine's specification version. Generally maps to the
  VM's major version.

**jvm/start_time**
  A gauge of the start time of the Java virtual machine in milliseconds since the epoch.

**jvm/application_time_millis**
  A gauge of the total time running application code since the process started in milliseconds.

**jvm/tenuring_threshold**
  A gauge of the number of times an object must survive GC in order to be promoted
  or 0 if the metric is unavailable.

**jvm/num_cpus**
  A gauge of the number of processors available to the JVM.

**jvm/fd_limit**
  (only available on Unix-based OS) A gauge of the maximum number of file descriptors.

**jvm/fd_count**
  (only available on Unix-based OS) A gauge of the number of open file descriptors.

**jvm/compilation/time_msec**
  A gauge of the elapsed time, in milliseconds, spent in compilation.

**jvm/classes/current_loaded**
  A gauge of the number of classes that are currently loaded.

**jvm/classes/total_loaded**
  A gauge of total number of classes that have been loaded since the JVM started.

**jvm/classes/total_unloaded**
  A gauge of total number of classes that have been unloaded since the JVM started.

**jvm/safepoint/count**
  A gauge of the number of safepoints taken place since the JVM started.

**jvm/safepoint/sync_time_millis**
  A gauge of the cumulative time, in milliseconds, spent getting all threads to
  safepoint states.

**jvm/safepoint/total_time_millis**
  A gauge of the cumulative time, in milliseconds, that the application has been
  stopped for safepoint operations.

**jvm/mem/metaspace/max_capacity**
  A gauge of the maximum size, in bytes, that the metaspace can grow to.

**jvm/heap/used**
  For the heap used for object allocation, a gauge of the current amount of memory used, in bytes.

**jvm/heap/committed**
  For the heap used for object allocation, a gauge of the amount of memory, in bytes,
  committed for the JVM to use.

**jvm/heap/max**
  For the heap used for object allocation, a gauge of the maximum amount of memory, in bytes,
  that can be used by the JVM.

**jvm/nonheap/used**
  For the non-heap memory, a gauge of the current amount of memory used, in bytes.

**jvm/nonheap/committed**
  For the non-heap memory, a gauge of the amount of memory, in bytes,
  committed for the JVM to use.

**jvm/nonheap/max**
  For the non-heap memory, a gauge of the maximum amount of memory, in bytes,
  that can be used by the JVM.

**jvm/mem/current/used**
  A gauge of the of the current memory used, in bytes, across all memory pools.

**jvm/mem/current/{memory_pool_name}/used**
  A gauge of the of the current memory used, in bytes, for the named memory pools.

**jvm/mem/current/{memory_pool_name}/max**
  A gauge of the of the maximum memory that can be used, in bytes, for the named memory pool.

**jvm/mem/postGC/used**
  A gauge of the memory used, in bytes, across all memory pools after the most recent
  garbage collection.

**jvm/mem/postGC/{memory_pool_name}/used**
  A gauge of the memory used, in bytes, for the named memory pool after the most recent
  garbage collection.

**jvm/mem/buffer/{buffer_pool_name}/count**
  A gauge of the number of buffers in the named pool. Example pool names include direct and mapped,
  though the names are subject to change.

**jvm/mem/buffer/{buffer_pool_name}/used**
  A gauge of the amount of memory used, in bytes, for the named pool. Example pool names
  include direct and mapped, though the names are subject to change.

**jvm/mem/buffer/{buffer_pool_name}/max**
  A gauge of the amount of memory capacity, in bytes, for the named pool. Example pool names
  include direct and mapped, though the names are subject to change.
