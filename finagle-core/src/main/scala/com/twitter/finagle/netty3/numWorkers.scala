package com.twitter.finagle.netty3

import com.twitter.app.GlobalFlag
import com.twitter.jvm.numProcs

object numWorkers extends GlobalFlag((numProcs() * 2).ceil.toInt, "Size of netty3 worker pool")