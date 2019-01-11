package com.twitter.finagle.stats

import com.twitter.app.GlobalFlag

object logOnShutdown
    extends GlobalFlag[Boolean](false, "Enable behavior to dump metrics via logger on shutdown")
