package com.twitter.finagle.protobuf.rpc.channel


class ClientSideDecoder(val repo: MethodLookup, val service: Service) extends FrameDecoder with ProtobufDecoder {

  @throws(classOf[Exception])
  def decode(ctx: ChannelHandlerContext, channel: Channel, buf: ChannelBuffer): Object = {
    decode(ctx, channel, buf, repo)
  }

  def getPrototype(methodName: String): Message = {
    val m = service.getDescriptorForType().findMethodByName(methodName)
    service.getResponsePrototype(m)
  }
}
