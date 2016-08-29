package com.twitter.finagle.netty4

import com.twitter.app.GlobalFlag
import com.twitter.jvm.numProcs

object numWorkers extends GlobalFlag((numProcs() * 2).ceil.toInt, "number of netty4 worker threads")