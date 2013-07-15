package com.twitter.finagle.stream

import com.twitter.finagle.{
  Codec, CodecFactory, Service, ServiceFactory, ServiceProxy, TooManyConcurrentRequestsException
}
import com.twitter.util.{Future, Promise, Time}
import org.jboss.netty.channel.{ChannelPipelineFactory, Channels}
import org.jboss.netty.handler.codec.http.{
  HttpClientCodec, HttpRequest, HttpServerCodec
}

/**
 * Don't release the underlying service until the response has
 * completed.
 */
private[stream] class DelayedReleaseService(self: Service[HttpRequest, StreamResponse])
  extends ServiceProxy[HttpRequest, StreamResponse](self)
{
  @volatile private[this] var done: Future[Unit] = Future.Done

  override def apply(req: HttpRequest) = {
    if (!done.isDefined)
      Future.exception(new TooManyConcurrentRequestsException)
    else {
      val p = new Promise[Unit]
      done = p
      self(req) map { res =>
        new StreamResponse {
          def httpResponse = res.httpResponse
          def messages = res.messages
          def error = res.error
          def release() {
            p.setValue(())
            res.release()
          }
        }
      } onFailure { _ => p.setValue(()) }
    }
  }

  override def close(deadline: Time) =
    done ensure { self.close(deadline) }
}

object Stream {
  def apply(): Stream = new Stream()
  def get() = apply()
}

class Stream extends CodecFactory[HttpRequest, StreamResponse] {
  def server = Function.const {
    new Codec[HttpRequest, StreamResponse] {
      def pipelineFactory = new ChannelPipelineFactory {
        def getPipeline = {
          val pipeline = Channels.pipeline()
          pipeline.addLast("httpCodec", new HttpServerCodec)
          pipeline.addLast("httpChunker", new HttpChunker)
          pipeline
        }
      }
    }
  }

 def client = Function.const {
    new Codec[HttpRequest, StreamResponse] {
      def pipelineFactory = new ChannelPipelineFactory {
        def getPipeline = {
          val pipeline = Channels.pipeline()
          pipeline.addLast("httpCodec", new HttpClientCodec)
          pipeline.addLast("httpDechunker", new HttpDechunker)
          pipeline
        }
      }

      override def prepareServiceFactory(
        underlying: ServiceFactory[HttpRequest, StreamResponse]
      ): ServiceFactory[HttpRequest, StreamResponse] =
        underlying map(new DelayedReleaseService(_))
    }
  }
}
