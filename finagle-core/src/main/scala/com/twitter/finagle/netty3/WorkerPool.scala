package com.twitter.finagle.netty3

import com.twitter.app.GlobalFlag
import com.twitter.jvm.numProcs
import org.jboss.netty.channel.socket.nio.NioWorkerPool

object numWorkers extends GlobalFlag((numProcs() * 2).ceil.toInt, "Size of netty3 worker pool")

object WorkerPool extends NioWorkerPool(Executor, numWorkers())
