package com.twitter.finagle.benchmark

import com.twitter.finagle.{Service, ChannelClosedException, ListeningServer}
import com.twitter.finagle.server.{DefaultServer, Listener}
import com.twitter.finagle.transport.Transport
import com.twitter.util._
import java.net.SocketAddress
import com.google.caliper.{SimpleBenchmark, Param}
import com.twitter.finagle.dispatch.SerialServerDispatcher

class EchoService[T] extends Service[T, T] {
  def apply(request: T) = Future.value(request)
}

/**
 * A Transport that allows a fixed number of reads. The output value of a read
 * is a constant provided by readVal. Writes to the transport are simply
 * dropped.
 */
class FixedNumReadsTransport[In, Out](readVal: Out, nReadsAllowed: Int) extends Transport[In, Out] {
  private[this] var nReads: Int = 0
  private[this] val closep = new Promise[Throwable]

  def write(input: In) = {
    if (!isOpen) Future.exception(Await.result(closep))
    else Future.Done
  }

  def read(): Future[Out] = {
    if (!isOpen) Future.exception(Await.result(closep))
    else synchronized {
      nReads += 1
      if (nReads <= nReadsAllowed) Future.value(readVal)
      else {
        close()
        closep flatMap Future.exception
      }
    }
  }

  def isOpen = !closep.isDefined

  def close(deadline: Time) = {
    if (!isOpen)
      closep.setException(new ChannelClosedException)
    closep map { _ => () }
  }

  val onClose = closep
  val localAddress = new SocketAddress {}
  val remoteAddress = new SocketAddress {}
}

/**
 * A load generator that implements Listener.
 * Calling loadGen() opens a new FixedNumReadsTransport which is then serviced
 * by serveTransport provided by the earlier listen() call.
 */
class LoadGeneratorListener extends Listener[Int, Int] {
  private[this] var _serveTransport: Transport[Int, Int] => Unit = null

  def listen(addr: SocketAddress)(serveTransport: Transport[Int, Int] => Unit) = {
    assert(_serveTransport == null)
    _serveTransport = serveTransport
    new ListeningServer with CloseAwaitably {
      def boundAddress = addr
      def closeServer(deadline: Time) = Future.Done
    }
  }

  def loadGen(nreqs: Int) {
    val trans = new FixedNumReadsTransport[Int, Int](0, nreqs)
    _serveTransport(trans)
  }
}

class DefaultServerBenchmark extends SimpleBenchmark {
  // Need to serve multiple requests to amortize the cost of setup
  // and tear-down of the transport and the dispatcher.
  @Param(Array("100")) val nreqs: Int = 100

  val listener = new LoadGeneratorListener
  object TestServer extends DefaultServer[Int, Int, Int, Int](
    "test", listener, new SerialServerDispatcher(_, _)
  )
  TestServer.serve(new SocketAddress {}, new EchoService[Int])

  def timeServerWithDefaultStack(n: Int) {
    var i = 0
    while (i < n) {
      listener.loadGen(nreqs)
      i += 1
    }
  }
}
