package com.twitter.finagle.httpx.codec

import org.jboss.netty.handler.codec.http.{DefaultHttpResponse, HttpResponseStatus, HttpVersion}

private[finagle] object OneHundredContinueResponse
  extends DefaultHttpResponse(
    HttpVersion.HTTP_1_1,
    HttpResponseStatus.CONTINUE)
