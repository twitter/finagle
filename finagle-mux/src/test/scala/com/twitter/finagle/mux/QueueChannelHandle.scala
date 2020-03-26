package com.twitter.finagle.mux

import com.twitter.concurrent.AsyncQueue
import com.twitter.finagle.{ChannelClosedException, Status}
import com.twitter.finagle.pushsession.{PushChannelHandle, PushSession, SentinelSession}
import com.twitter.finagle.ssl.session.{NullSslSessionInfo, SslSessionInfo}
import com.twitter.finagle.util.Updater
import com.twitter.util.{Future, Promise, Return, Throw, Time, Try}
import java.net.SocketAddress
import java.util.concurrent.Executor
import scala.util.control.NonFatal

/** A mocked channel handle useful for testing */
private[mux] class QueueChannelHandle[In, Out](destinationQueue: AsyncQueue[Out])
    extends PushChannelHandle[In, Out] {
  private[this] val inLocalExecutor = new ThreadLocal[Boolean] {
    override def initialValue(): Boolean = false
  }

  private[this] val closed = {
    val p = Promise[Unit]()
    // Make sure we clean up our queue when we close
    p.respond {
      case Return(_) => destinationQueue.fail(new ChannelClosedException())
      case Throw(t) => destinationQueue.fail(t)
    }
    p
  }

  // Must only be changed from within the serial executor
  private[this] var currentSession: PushSession[In, Out] = SentinelSession(this)

  private[this] def inExecutor: Boolean = inLocalExecutor.get()

  /** Receive a message from the I/O engine */
  def sessionReceive(msg: In): Unit =
    serialExecutor.execute(new Runnable { def run(): Unit = currentSession.receive(msg) })

  /** Fail the handle based on an I/O engine reason */
  def failHandle(cause: Try[Unit]): Unit =
    serialExecutor.execute(new Runnable { def run(): Unit = closed.updateIfEmpty(cause) })

  val remoteAddress: SocketAddress = new SocketAddress {}
  val localAddress: SocketAddress = new SocketAddress {}
  val serialExecutor: Executor = {
    val updater = new Updater[Runnable] {
      protected def preprocess(elems: Seq[Runnable]): Seq[Runnable] = elems
      protected def handle(elem: Runnable): Unit = {
        inLocalExecutor.set(true)
        try elem.run()
        catch { case NonFatal(t) => closed.updateIfEmpty(Throw(t)) }
        finally inLocalExecutor.set(
          false
        )
      }
    }

    new Executor { def execute(command: Runnable): Unit = updater(command) }
  }

  def registerSession(newSession: PushSession[In, Out]): Unit = {
    if (!inExecutor) {
      val ex = new IllegalStateException(
        "Attempted to register session outside the serial executor"
      )
      closed.updateIfEmpty(Throw(ex))
      throw ex
    } else {
      currentSession = newSession
    }
  }

  def send(message: Out)(onComplete: Try[Unit] => Unit): Unit =
    send(message :: Nil)(onComplete)

  def send(messages: Iterable[Out])(onComplete: Try[Unit] => Unit): Unit = {
    serialExecutor.execute(new Runnable {
      def run(): Unit = {
        if (closed.isDefined) onComplete(Throw(new ChannelClosedException()))
        if (messages.forall(destinationQueue.offer)) {
          onComplete(Return.Unit)
        } else {
          onComplete(Throw(new ChannelClosedException()))
        }
      }
    })
  }

  def sendAndForget(message: Out): Unit =
    sendAndForget(message :: Nil)

  def sendAndForget(messages: Iterable[Out]): Unit = {
    serialExecutor.execute(new Runnable {
      def run(): Unit = {
        // If we can't enqueue them all we're closed
        if (!messages.forall(destinationQueue.offer)) {
          closed.setDone()
        }
      }
    })
  }

  def sslSessionInfo: SslSessionInfo = NullSslSessionInfo

  def status: Status = if (closed.isDefined) Status.Closed else Status.Open

  def onClose: Future[Unit] = closed

  def close(deadline: Time): Future[Unit] = {
    serialExecutor.execute(new Runnable { def run(): Unit = closed.setDone() })
    onClose
  }
}
