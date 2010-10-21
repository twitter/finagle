package com.twitter.finagle.channel

import org.specs.Specification
import org.specs.mock.Mockito
import org.jboss.netty.channel._

class PoolingBrokerSpec extends Specification with Mockito {
  "PoolingBroker" should {
    val channelConnector = mock[ChannelConnector]
    val reservedChannel = mock[Channel]
    val brokeredChannel = mock[BrokeredChannel]
    val pool = mock[ChannelPool]
    val reserveFuture = new DefaultChannelFuture(reservedChannel, false)
    pool.reserve() returns reserveFuture
    val writeFuture = new DefaultChannelFuture(reservedChannel, false)
    val e = mock[MessageEvent]
    channelConnector(brokeredChannel, reservedChannel, e) returns writeFuture
    val poolingBroker = new PoolingBroker(channelConnector, pool)

    "dispatch reserves and releases connection from the pool" in {
      poolingBroker.dispatch(brokeredChannel, e)
      reserveFuture.setSuccess()
      writeFuture.setSuccess()
      there was one(pool).reserve()
      there was one(pool).release(reservedChannel)
    }
  }
}