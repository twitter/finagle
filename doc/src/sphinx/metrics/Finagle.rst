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
  A gauge of the current number of DNS resolutions waiting for lookup
  :src:`InetResolver <com/twitter/finagle/Resolver.scala>`.
