//#serverpipeline
import io.netty.handler.codec.string.{StringEncoder, StringDecoder}
import io.netty.channel._
import io.netty.handler.codec.frame.{Delimiters, DelimiterBasedFrameDecoder}
import io.netty.util.CharsetUtil

object StringServerPipeline extends ChannelPipelineFactory {
  def getPipeline = {
    val pipeline = Channels.pipeline()
    pipeline.addLast("line", new DelimiterBasedFrameDecoder(100, Delimiters.lineDelimiter: _*))
    pipeline.addLast("stringDecoder", new StringDecoder(CharsetUtil.UTF_8))
    pipeline.addLast("stringEncoder", new StringEncoder(CharsetUtil.UTF_8))
    pipeline
  }
}
//#serverpipeline

//#clientpipeline
import io.netty.handler.codec.string.{StringEncoder, StringDecoder}
import io.netty.channel._
import io.netty.handler.codec.frame.{Delimiters, DelimiterBasedFrameDecoder}
import io.netty.util.CharsetUtil

object StringClientPipeline extends ChannelPipelineFactory {
  def getPipeline = {
    val pipeline = Channels.pipeline()
    pipeline.addLast("stringEncode", new StringEncoder(CharsetUtil.UTF_8))
    pipeline.addLast("stringDecode", new StringDecoder(CharsetUtil.UTF_8))
    pipeline.addLast("line", new DelimEncoder('\n'))
    pipeline
  }
}

class DelimEncoder(delim: Char) extends SimpleChannelHandler {
  override def writeRequested(ctx: ChannelHandlerContext, evt: MessageEvent) = {
    val newMessage = evt.getMessage match {
      case m: String => m + delim
      case m => m
    }
    Channels.write(ctx, evt.getFuture, newMessage, evt.getRemoteAddress)
  }
}
//#clientpipeline
