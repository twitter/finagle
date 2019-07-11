package com.twitter.finagle.offload

import com.twitter.app.GlobalFlag

object numWorkers
    extends GlobalFlag[Int](
      """Experimental flag. Enables the offload filter using a thread pool with the specified number of threads.
    | When this flag is greater than zero, the execution of application code happens in an isolated pool and the netty threads
    | are used only to handle the network channels. This behavior changes the assumptions regarding the scheduling of tasks in
    | finagle applications. Traditionally, the recommendation is to execute CPU-intensive tasks using a `FuturePool` but, when
    | this flag is enabled, CPU-intensive tasks don't require a `FuturePool`. Important: Blocking tasks should still use a
    | `FuturePool`.
    |
    | It's important to review the allocation of thread pools when this flag is enabled otherwise the application might create
    | too many threads, which leads to more GC pressure and increases the risk of CPU throttling.
  """.stripMargin
    )
