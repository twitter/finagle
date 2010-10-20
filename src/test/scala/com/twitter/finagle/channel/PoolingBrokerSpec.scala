package com.twitter.finagle.channel

import org.specs.Specification
import org.specs.mock.Mockito

class PoolingBrokerSpec extends Specification with Mockito {
  "PoolingBroker" should {
    val handlingChannel = mock[BrokeredChannel]
    val channelPool = mock[ChannelPool]
    val poolingBroker = new PoolingBroker(channelPool)

    "dispatch reserves a connection from the pool" in {
      poolingBroker.dispatch(handlingChannel, e)
    }
  }
}