package com.twitter.finagle.stream

import com.twitter.finagle.netty3.transport.ChannelTransport
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.transport.Transport
import com.twitter.finagle._
import com.twitter.util.{Future, Promise, Time, Closable}
import org.jboss.netty.channel.{ChannelPipelineFactory, Channels, Channel}
import org.jboss.netty.handler.codec.http.{HttpClientCodec, HttpServerCodec}

/**
 * Don't release the underlying service until the response has completed.
 */
private[stream] class DelayedReleaseService[Req](self: Service[Req, StreamResponse])
  extends ServiceProxy[Req, StreamResponse](self)
{
  @volatile private[this] var done: Future[Unit] = Future.Done

  override def apply(req: Req) = {
    if (!done.isDefined)
      Future.exception(new TooManyConcurrentRequestsException)
    else {
      val p = new Promise[Unit]
      done = p
      self(req) map { res =>
        new StreamResponse {
          def info = res.info
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
  def apply[Req: RequestType](): Stream[Req] = new Stream()
  def get[Req: RequestType](): Stream[Req] = apply()
}

class Stream[Req: RequestType] extends CodecFactory[Req, StreamResponse] {
  def server: Server = Function.const {
    new Codec[Req, StreamResponse] {
      def pipelineFactory = new ChannelPipelineFactory {
        def getPipeline = {
          val pipeline = Channels.pipeline()
          pipeline.addLast("httpCodec", new HttpServerCodec)
          pipeline
        }
      }

      override def newServerDispatcher(
          transport: Transport[Any, Any],
          service: Service[Req, StreamResponse]): Closable =
        new StreamServerDispatcher(transport, service)
    }
  }

 def client: Client = Function.const {
    new Codec[Req, StreamResponse] {
      def pipelineFactory = new ChannelPipelineFactory {
        def getPipeline = {
          val pipeline = Channels.pipeline()
          pipeline.addLast("httpCodec", new HttpClientCodec)
          pipeline
        }
      }

     override def newClientDispatcher(trans: Transport[Any, Any])
       : Service[Req, StreamResponse] = new StreamClientDispatcher(trans)

      // TODO: remove when the Meta[_] patch lands.
      override def prepareServiceFactory(
        underlying: ServiceFactory[Req, StreamResponse]
      ): ServiceFactory[Req, StreamResponse] =
        underlying map(new DelayedReleaseService(_))

      // TODO: remove when ChannelTransport is the default for clients.
      override def newClientTransport(
          ch: Channel, statsReceiver: StatsReceiver): Transport[Any, Any] =
        new ChannelTransport(ch)

    }
  }

  override val protocolLibraryName: String = "http-stream"
}
