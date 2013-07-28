package com.finagle.zookeeper

import org.jboss.netty.channel.ChannelPipelineFactory
import com.twitter.finagle.Client
import com.twitter.finagle.{ServiceFactory, Group, CodecFactory}
import com.twitter.finagle.netty3.Netty3Transporter
import com.twitter.finagle.client.{Bridge, DefaultClient}
import com.twitter.finagle.dispatch.SerialClientDispatcher
import java.net.SocketAddress

object Zookeeper extends Client[ZookeeperRequest, ZookeeperResponse] {

  def newClient(group: Group[SocketAddress]): ServiceFactory[ZookeeperRequest, ZookeeperResponse] =
    ZookeeperClient.newClient(group)
}

object ZookeeperClient extends DefaultClient[ZookeeperRequest, ZookeeperResponse](
  name = "zookeeper",
  endpointer = Bridge[ZookeeperRequest, ZookeeperResponse, ZookeeperRequest, ZookeeperResponse](
    ZookeeperTransporter,
  //TODO: May be safe to use piplelining since requests are sequenced at server side.
    new SerialClientDispatcher(_)
  )
)

object ZookeeperTransporter extends Netty3Transporter[ZookeeperRequest, ZookeeperResponse](
  "zookeeper",
  ZookeeperClientPipelineFactory
)

object ZookeeperClientPipelineFactory extends ChannelPipelineFactory{

  def getPipeline() = {
    null
  }

}
