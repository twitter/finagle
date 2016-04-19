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
