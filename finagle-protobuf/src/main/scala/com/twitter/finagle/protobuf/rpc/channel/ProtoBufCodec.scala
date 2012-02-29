package com.twitter.finagle.protobuf.rpc.channel

import org.jboss.netty.channel.ChannelPipelineFactory
import org.jboss.netty.channel.Channels
import com.google.protobuf.Message
import com.google.protobuf.Service
import com.twitter.conversions.storage.intToStorageUnitableWholeNumber
import com.twitter.finagle.Codec
import com.twitter.finagle.CodecFactory

class ProtoBufCodec(val service: Service) extends CodecFactory[(String, Message), (String, Message)] {

  val maxFrameSize = 1.megabytes.inBytes.intValue

  val repo = SimpleMethodLookup(service)

  def server = Function.const {
    new Codec[(String, Message), (String, Message)] {
      def pipelineFactory = new ChannelPipelineFactory {
        def getPipeline = {
          val pipeline = Channels.pipeline()
          pipeline.addLast("decoder", new ServerSideDecoder(repo, service))
          pipeline.addLast("encoder", new CustomProtobufEncoder(repo));
          pipeline
        }
      }
    }
  }

  def client = Function.const {
    new Codec[(String, Message), (String, Message)] {
      def pipelineFactory = new ChannelPipelineFactory {
        def getPipeline = {
          val pipeline = Channels.pipeline()
          pipeline.addLast("encoder", new CustomProtobufEncoder(repo))
          pipeline.addLast("decoder",
            new ClientSideDecoder(repo, service))
          pipeline
        }
      }
    }
  }

}
