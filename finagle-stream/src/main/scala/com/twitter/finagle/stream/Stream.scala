package com.twitter.finagle.stream

import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.transport.{Transport, ChannelTransport}
import com.twitter.finagle.{
  Codec, CodecFactory, Service, ServiceFactory, ServiceProxy, TooManyConcurrentAsksException}
import com.twitter.util.{Future, Promise, Time, Closable}
import org.jboss.netty.channel.{ChannelPipelineFactory, Channels, Channel}
import org.jboss.netty.handler.codec.http.{HttpClientCodec, HttpRequest => HttpAsk, HttpServerCodec}

/**
 * Don't release the underlying service until the response has
 * completed.
 */
private[stream] class DelayedReleaseService(self: Service[HttpAsk, StreamResponse])
  extends ServiceProxy[HttpAsk, StreamResponse](self)
{
  @volatile private[this] var done: Future[Unit] = Future.Done

  override def apply(req: HttpAsk) = {
    if (!done.isDefined)
      Future.exception(new TooManyConcurrentAsksException)
    else {
      val p = new Promise[Unit]
      done = p
      self(req) map { res =>
        new StreamResponse {
          def httpResponse = res.httpResponse
          def messages = res.messages
          def error = res.error
          def release() {
            p.setDone()
            res.release()
          }
        }
      } onFailure { _ => p.setDone() }
    }
  }

  override def close(deadline: Time) =
    done ensure { self.close(deadline) }
}

object Stream {
  def apply(): Stream = new Stream()
  def get() = apply()
}

class Stream extends CodecFactory[HttpAsk, StreamResponse] {
  def server = Function.const {
    new Codec[HttpAsk, StreamResponse] {
      def pipelineFactory = new ChannelPipelineFactory {
        def getPipeline = {
          val pipeline = Channels.pipeline()
          pipeline.addLast("httpCodec", new HttpServerCodec)
          pipeline
        }
      }

      override def newServerDispatcher(
          transport: Transport[Any, Any],
          service: Service[HttpAsk, StreamResponse]): Closable =
        new StreamServerDispatcher(transport, service)
    }
  }

 def client = Function.const {
    new Codec[HttpAsk, StreamResponse] {
      def pipelineFactory = new ChannelPipelineFactory {
        def getPipeline = {
          val pipeline = Channels.pipeline()
          pipeline.addLast("httpCodec", new HttpClientCodec)
          pipeline
        }
      }

     override def newClientDispatcher(trans: Transport[Any, Any])
       : Service[HttpAsk, StreamResponse] = new StreamClientDispatcher(trans)

      // TODO: remove when the Meta[_] patch lands.
      override def prepareServiceFactory(
        underlying: ServiceFactory[HttpAsk, StreamResponse]
      ): ServiceFactory[HttpAsk, StreamResponse] =
        underlying map(new DelayedReleaseService(_))

      // TODO: remove when ChannelTransport is the default for clients.
      override def newClientTransport(
          ch: Channel, statsReceiver: StatsReceiver): Transport[Any, Any] =
        new ChannelTransport(ch)

    }
  }
}
