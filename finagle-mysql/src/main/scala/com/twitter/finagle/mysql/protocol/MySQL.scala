package com.twitter.finagle.mysql.protocol

import org.jboss.netty.channel._
import com.twitter.finagle._

class MySQL(username: String, password: String) extends CodecFactory[Request, Result] {

  def server = throw new Exception("Not yet implemented...")

  def client = Function.const {
    new Codec[Request, Result] {

      def pipelineFactory = new ChannelPipelineFactory {
        def getPipeline = {
          val pipeline = Channels.pipeline()

          pipeline.addLast("decoder", new Decoder)
          pipeline.addLast("encoder", Encoder)
          pipeline.addLast("authentication", new AuthenticationHandler(username, password))

          pipeline
        }
      }
    }
  }
}