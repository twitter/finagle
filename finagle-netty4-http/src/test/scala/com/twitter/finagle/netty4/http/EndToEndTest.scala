package com.twitter.finagle.netty4.http

import com.twitter.finagle.http.AbstractEndToEndTest
import com.twitter.finagle
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class EndToEndTest extends AbstractEndToEndTest {
  def implName: String = "netty4 http/1.1"
  def clientImpl(): finagle.Http.Client = finagle.Http.client.configured(exp.Netty4Impl)

  def serverImpl(): finagle.Http.Server = finagle.Http.server.configured(exp.Netty4Impl)

  def featureImplemented(feature: Feature): Boolean =
    !Set(ClientAbort, TooLongStream, CloseStream)(feature)
}
