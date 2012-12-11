package com.twitter.finagle.netty3

import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito
import org.jboss.netty.channel._

class RefcountedSpec extends SpecificationWithJUnit with Mockito {
  "NewChannelFactory" should {
    val underlying = mock[ChannelFactory]
    val make = mock[() => ChannelFactory]
    make() returns underlying
    val newCf = new NewChannelFactory(make)

    "Create CFs on demand, proxying newChannel" in {
      there was no(make).apply()
      val cf = newCf()
      there was one(make).apply()
      there was no(underlying).releaseExternalResources()
      there was no(underlying).newChannel(any)
      val pipeline = mock[ChannelPipeline]
      cf.newChannel(pipeline)
      there was one(underlying).newChannel(pipeline)
    }

    "Release CFs when all refs are, recreating on demand" in {
      there was no(make).apply()
      val cf0, cf1 = newCf()
      there was one(make).apply()
      cf0.releaseExternalResources()
      there was no(underlying).releaseExternalResources()
      cf1.releaseExternalResources()
      there was one(underlying).releaseExternalResources()

      val cf = newCf()
      there were two(make).apply()
      cf.releaseExternalResources()
      there were two(underlying).releaseExternalResources()
    }
  }
}
