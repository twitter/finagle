package com.twitter.finagle.protobuf.rpc.channel


class ServerSideDecoder(val repo: MethodLookup, val service: Service) extends FrameDecoder with ProtobufDecoder {

  @throws(classOf[Exception])
  def decode(ctx: ChannelHandlerContext, channel: Channel, buf: ChannelBuffer): Object = {
    decode(ctx, channel, buf, repo)
  }

  def getPrototype(methodName: String): Message = {
    val m = service.getDescriptorForType().findMethodByName(methodName)
    service.getRequestPrototype(m)
  }
}
