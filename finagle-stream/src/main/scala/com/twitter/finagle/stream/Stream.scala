package com.twitter.finagle.stream

import com.twitter.finagle.dispatch.GenSerialClientDispatcher
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

     override def newClientDispatcher(
       trans: Transport[Any, Any],
       params: Stack.Params
     ): Service[Req, StreamResponse] =
       new StreamClientDispatcher(
         trans,
         params[param.Stats].statsReceiver.scope(GenSerialClientDispatcher.StatsScope)
       )

      // TODO: remove when the Meta[_] patch lands.
      override def prepareServiceFactory(
        underlying: ServiceFactory[Req, StreamResponse]
      ): ServiceFactory[Req, StreamResponse] =
        underlying map(new DelayedReleaseService(_))

      // TODO: remove when ChannelTransport is the default for clients.
      override def newClientTransport(
          ch: Channel, statsReceiver: StatsReceiver): Transport[Any, Any] =
        new ChannelTransport(ch)

      override def protocolLibraryName: String = Stream.this.protocolLibraryName

    }
  }

  override val protocolLibraryName: String = "http-stream"
}

/**
 * Indicates that a stream has ended.
 */
object EOF extends Exception

/**
 * HTTP header encoded as a string pair.
 */
final class Header private(val key: String, val value: String) {
  override def toString: String = s"Header($key, $value)"
  override def equals(o: Any): Boolean = o match {
    case h: Header => h.key == key && h.value == value
    case _ => false
  }
}

object Header {
  implicit class Ops(val headers: Seq[Header]) extends AnyVal {
    /**
     * The value of the first header found matching this key, or None.
     */
    def first(key: String): Option[String] =
      headers.find(_.key == key.toLowerCase).map(_.value)
  }

  def apply(key: String, value: Any): Header =
    new Header(key.toLowerCase, value.toString)
}

/**
 * Represents the HTTP version.
 */
case class Version(major: Int, minor: Int)

trait RequestType[Req] {
  def canonize(req: Req): StreamRequest
  def specialize(req: StreamRequest): Req
}
