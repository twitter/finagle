package com.twitter.finagle.offload

import com.twitter.app.GlobalFlag

object queueSize
    extends GlobalFlag[Int](
      Int.MaxValue,
      "Experimental flag. When offload filter is enabled, its queue is bounded by this value (default is" +
        "Int.MaxValue or unbounded). Any excess work that can't be offloaded due to the queue overflow is run" +
        "on IO (Netty) threads instead. Thus, when set, this flag enforces the backpressure on the link between" +
        "Netty (producer) and your application (consumer)."
    )
