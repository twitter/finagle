package com.twitter.finagle

import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito

import java.net.SocketAddress

class ExceptionsSpec extends SpecificationWithJUnit with Mockito {
  "ChannelException" should {
    val address = mock[SocketAddress]
    address.toString returns "foo"
    val underlying = mock[Throwable]
    underlying.getMessage returns "bar"

    "not generate message when all parameters are null" in {
      val ex = new ChannelException(null, null)
      ex.getMessage must beNull
    }

    "generate message with address info when address is provided" in {
      val ex = new ChannelException(null, address)
      ex.getMessage.contains("foo") must beTrue
    }

    "generate message with underlying exception info when exception is provided" in {
      val ex = new ChannelException(underlying, null)
      ex.getMessage mustNot beNull
    }

    "generate message with correct info when all parameters are provided" in {
      val ex = new ChannelException(underlying, address)
      ex.getMessage.contains("foo") must beTrue
      ex.getMessage.contains("bar") must beTrue
    }

    "provide access to remote address" in {
      val ex = new ChannelException(underlying, address)
      ex.remoteAddress mustEqual address
    }
  }

  "WriteException" should {
    "apply and unapply" in {
      val rootCause = new RuntimeException("howdy")
      val writeEx = WriteException(rootCause)
      writeEx.getCause mustEq(rootCause)

      writeEx match {
        case WriteException(cause) => cause mustEqual(rootCause)
      }
    }

    "no cause" in {
      val writeEx = WriteException(null)
      writeEx match {
        case WriteException(cause) => cause must beNull
      }
    }
  }

}
