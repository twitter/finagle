package com.twitter.finagle.util

import org.specs.Specification

import org.jboss.netty.channel.{Channels, ChannelFuture}

import Conversions._

object ChannelFutureSpec extends Specification {
  "joining" should {
    "return when all futures are satisfied" in {
      val f0 = Channels.future(null)
      val f1 = Channels.future(null)

      val joined = f0 joinWith f1

      joined.isDone must beFalse
      f0.setSuccess()
      joined.isDone must beFalse
      f1.setSuccess()
      joined.isDone must beTrue
      joined.isSuccess must beTrue
    }

    "return an error if the first future errored out" in {
      val f0 = Channels.future(null)
      val f1 = Channels.future(null)

      val joined = f0 joinWith f1

      joined.isDone must beFalse
      val exc = new Exception
      f0.setFailure(exc)
      joined.isDone must beFalse
      f1.setSuccess()
      joined.isDone must beTrue
      joined.isSuccess must beFalse
      joined.getCause must be_==(exc)
    }

    "return an error if the second future errored out" in {
      val f0 = Channels.future(null)
      val f1 = Channels.future(null)

      val joined = f0 joinWith f1

      joined.isDone must beFalse
      val exc = new Exception
      f0.setSuccess()
      joined.isDone must beFalse
      f1.setFailure(exc)
      joined.isDone must beTrue
      joined.isSuccess must beFalse
      joined.getCause must be_==(exc)
    }
  }
}
