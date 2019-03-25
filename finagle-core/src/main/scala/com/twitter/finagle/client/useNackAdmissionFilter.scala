package com.twitter.finagle.client

import com.twitter.app.GlobalFlag

object useNackAdmissionFilter
    extends GlobalFlag[Boolean](
      true,
      "When false, globally disables client side nack admission control (NackAdmissionFilter)")
