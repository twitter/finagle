package com.twitter.finagle.offload

import com.twitter.app.GlobalFlag
import com.twitter.util.Duration

object statsSampleInterval
    extends GlobalFlag[Duration](
      Duration.fromMilliseconds(100),
      "Sample offload filter queue at this interval and record task delay (in milliseconds) under " +
        "offload_pool/delay_ms stat (default: 100ms). Setting this flag to either 'duration.top', " +
        "'duration.bottom', or zero disables both the stat and sampling."
    )
