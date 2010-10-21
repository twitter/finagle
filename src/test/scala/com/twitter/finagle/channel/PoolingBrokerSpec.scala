package com.twitter.finagle.channel

import org.specs.Specification
import org.specs.mock.Mockito
import org.jboss.netty.channel._

class PoolingBrokerSpec extends Specification with Mockito {
  "PoolingBroker" should {
    val someMessage = mock[Object]
    val reservedChannel = mock[Channel]
    val pool = mock[ChannelPool]
    val reserveFuture = new DefaultChannelFuture(reservedChannel, false)
    pool.reserve() returns reserveFuture
    val brokeredChannel = new BrokeredChannelFactory().newChannel(Channels.pipeline())
    brokeredChannel.connect(new PoolingBroker(pool))

    "dispatch reserves and releases connection from the pool" in {
      Channels.write(brokeredChannel, someMessage)

      there was one(pool).reserve()
      there was one(pool).release(reservedChannel)
    }
  }
}