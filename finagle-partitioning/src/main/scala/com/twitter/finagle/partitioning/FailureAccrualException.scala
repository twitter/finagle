package com.twitter.finagle.partitioning

import com.twitter.finagle._

class FailureAccrualException(message: String) extends RequestException(message, cause = null)
