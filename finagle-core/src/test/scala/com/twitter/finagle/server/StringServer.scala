package com.twitter.finagle.server

import com.twitter.conversions.time._
import com.twitter.finagle._
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.dispatch.SerialServerDispatcher
import com.twitter.finagle.netty3.Netty3Listener
import com.twitter.io.Charsets
import com.twitter.util.{Promise, Await}
import java.net.{SocketAddress, Socket, InetSocketAddress, InetAddress}
import org.jboss.netty.channel._
import org.jboss.netty.handler.codec.frame.{Delimiters, DelimiterBasedFrameDecoder}
import org.jboss.netty.handler.codec.string.{StringEncoder, StringDecoder}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.junit.JUnitRunner
import scala.util.control.NonFatal

private[finagle] object StringServerPipeline extends ChannelPipelineFactory {
  def getPipeline = {
    val pipeline = Channels.pipeline()
    pipeline.addLast("line", new DelimiterBasedFrameDecoder(100, Delimiters.lineDelimiter: _*))
    pipeline.addLast("stringDecoder", new StringDecoder(Charsets.Utf8))
    pipeline.addLast("stringEncoder", new StringEncoder(Charsets.Utf8))
    pipeline
  }
}

private[finagle] trait StringServer {
  case class Server(stack: Stack[ServiceFactory[String, String]] = StackServer.newStack,
      params: Stack.Params = StackServer.defaultParams)
    extends StdStackServer[String, String, Server] {
    protected def copy1(
      stack: Stack[ServiceFactory[String, String]] = this.stack,
      params: Stack.Params = this.params
    ) = copy(stack, params)

    protected type In = String
    protected type Out = String

    protected def newListener() = Netty3Listener(StringServerPipeline, params)
    protected def newDispatcher(transport: Transport[In, Out], service: Service[String, String]) =
      new SerialServerDispatcher(transport, service)
  }

  val stringServer = Server()
}

@RunWith(classOf[JUnitRunner])
class StringServerTest extends FunSuite with StringServer with Eventually with IntegrationPatience {
  test("StringServer notices when the client cuts the connection") {
    val p = Promise[String]()
    @volatile var interrupted = false
    p.setInterruptHandler { case NonFatal(t) =>
      interrupted = true
    }
    @volatile var observedRequest: Option[String] = None

    val service = new Service[String, String] {
      def apply(request: String) = {
        observedRequest = Some(request)
        p
      }
    }

    val server = stringServer.serve(
      new InetSocketAddress(InetAddress.getLoopbackAddress, 0),
      service)

    val client = new Socket()
    eventually { client.connect(server.boundAddress) }

    client.getOutputStream.write("hello netty4!\n".getBytes("UTF-8"))
    client.getOutputStream.flush()
    eventually { assert(observedRequest == Some("hello netty4!")) }

    client.close()
    eventually { assert(interrupted) }

    Await.ready(server.close(), 2.seconds)
  }
}
