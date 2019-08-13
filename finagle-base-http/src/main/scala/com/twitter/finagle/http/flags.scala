package com.twitter.finagle.http

import com.twitter.app.GlobalFlag

object serverErrorsAsFailures
    extends GlobalFlag(
      true,
      "Treat responses with status codes in " +
        "the 500s as failures. See " +
        "`com.twitter.finagle.http.service.HttpResponseClassifier.ServerErrorsAsFailures`"
    )
