package com.twitter.finagle.offload

import com.twitter.app.GlobalFlag

object maxQueueLength
    extends GlobalFlag[Int](
      default = Int.MaxValue,
      help =
        "Experimental flag. Sets the maximum number of jobs in the thread pool in the offload filter"
    )
