package com.twitter.finagle.postgresql.mock

import com.twitter.finagle.Status
import com.twitter.finagle.postgresql.BackendMessage
import com.twitter.finagle.postgresql.FrontendMessage
import com.twitter.finagle.postgresql.mock.MockPgTransport.Step
import com.twitter.finagle.ssl.session.NullSslSessionInfo
import com.twitter.finagle.transport.SimpleTransportContext
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.transport.TransportContext
import com.twitter.util.Future
import com.twitter.util.Promise
import com.twitter.util.Return
import com.twitter.util.Throw
import com.twitter.util.Time
import java.net.SocketAddress

final class MockPgTransport(
  steps: Seq[MockPgTransport.Step],
) extends Transport[FrontendMessage, BackendMessage]
    with org.scalatest.Assertions {
  override type Context = TransportContext

  private[this] val closep = new Promise[Throwable]

  val onClose: Future[Throwable] = closep
  val context: TransportContext =
    new SimpleTransportContext(new SocketAddress {}, new SocketAddress {}, NullSslSessionInfo)

  private[this] val states: Seq[State] = steps.flatMap(mkStates(_))
  private[this] val iter: Iterator[State] = states.iterator

  private[this] var state: State = _
  advanceState()

  def write(msg: FrontendMessage): Future[Unit] = {
    val result = state.write(msg)
    advanceState()
    result
  }

  def read(): Future[BackendMessage] = {
    val result = state.read()
    advanceState()
    result
  }

  def resume(tag: String): Unit = {
    state.resume(tag)
    advanceState()
  }

  def status: Status = if (closep.isDefined) Status.Closed else Status.Open

  def close(deadline: Time): Future[Unit] = {
    val ex = new Exception("QueueTransport is now closed")
    closep.updateIfEmpty(Return(ex))
    Future.Done
  }

  private def mkStates(step: MockPgTransport.Step): Seq[State] = step match {
    case Step.Expect(frontendMessage, backendMessages) =>
      Seq(new WriteState(frontendMessage), new ReadState(backendMessages))
    case Step.Write(frontendMessages) =>
      Seq(new WriteState(frontendMessages))
    case Step.Read(backendMessages) =>
      Seq(new ReadState(backendMessages))
    case Step.Suspend(tag) =>
      Seq(new SuspendState(tag))
  }

  private def advanceState(): Unit = {
    while (state == null || state.isCompleted) {
      if (iter.hasNext) {
        state = iter.next()
      } else {
        state = EOFState
      }
    }
  }

  private abstract class State {
    def isCompleted: Boolean

    def read(): Future[BackendMessage]
    def write(msg: FrontendMessage): Future[Unit]
    def resume(tag: String): Unit
  }

  private class WriteState(
    val msgs: Seq[FrontendMessage],
  ) extends State {
    val iter = msgs.iterator
    var nextWrite: Option[FrontendMessage] = _

    private def advance(): Unit = {
      if (iter.hasNext) {
        nextWrite = Some(iter.next())
      } else {
        nextWrite = None
      }
    }

    advance()

    override def isCompleted: Boolean = nextWrite.isEmpty

    def write(msg: FrontendMessage): Future[Unit] = Future {
      assert(msg == nextWrite.get)
      advance()
    }

    override def read(): Future[BackendMessage] = Future {
      fail(s"Expected a write('${nextWrite.get}), but got read instead")
    }

    override def resume(tag: String): Unit = Future {
      fail(s"Expected a write(${nextWrite.get}), but got resume instead")
    }
  }

  private class ReadState(
    val msgs: Seq[BackendMessage],
  ) extends State {
    val iter = msgs.iterator
    var nextRead: Option[BackendMessage] = _

    advance()

    private def advance(): Unit = {
      if (iter.hasNext) {
        nextRead = Some(iter.next())
      } else {
        nextRead = None
      }
    }

    override def isCompleted: Boolean = nextRead.isEmpty

    def write(msg: FrontendMessage): Future[Unit] = Future {
      fail(s"Expected a read of ${nextRead.get}, but got write($msg) instead")
    }

    override def read(): Future[BackendMessage] = Future {
      val msg = nextRead.get
      advance()
      msg
    }

    override def resume(tag: String): Unit = Future {
      fail(s"Expected a read of ${nextRead.get}, but got resume(${tag}) instead")
    }
  }

  private class SuspendState(
    tag: String,
  ) extends State {
    private val promise = new Promise[Unit]

    onClose.respond {
      case Return(ex) => promise.updateIfEmpty(Throw(ex))
      case Throw(ex) => promise.updateIfEmpty(Throw(ex))
    }

    override def isCompleted: Boolean = promise.isDefined

    override def write(msg: FrontendMessage): Future[Unit] = {
      promise.masked.flatMap { _ => MockPgTransport.this.write(msg) }
    }

    override def read(): Future[BackendMessage] = {
      promise.masked.flatMap { _ => MockPgTransport.this.read() }
    }

    override def resume(tag: String): Unit = {
      assert(tag == this.tag)
      promise.updateIfEmpty(Return(()))
    }
  }

  private object EOFState extends State {
    override def isCompleted: Boolean = false

    override def write(msg: FrontendMessage): Future[Unit] = Future {
      fail(s"expected EOF got write(${msg})")
    }

    override def read(): Future[BackendMessage] = Future {
      fail(s"expected EOF got read()")
    }

    override def resume(tag: String): Unit = {
      fail(s"expected EOF got resume(${tag})")
    }
  }
}

object MockPgTransport {
  def expect(
    frontendMessage: FrontendMessage,
    backendMessages: BackendMessage*
  ): Step.Expect = {
    Step.Expect(Seq(frontendMessage), backendMessages)
  }

  def expect(
    frontendMessage: Seq[FrontendMessage],
    backendMessages: Seq[BackendMessage]
  ): Step.Expect = {
    Step.Expect(frontendMessage, backendMessages)
  }

  def suspend(tag: String): Step.Suspend = {
    Step.Suspend(tag)
  }

  def write(frontendMessages: FrontendMessage*): Step.Write = {
    Step.Write(frontendMessages)
  }

  def read(backendMessages: BackendMessage*): Step.Read = {
    Step.Read(backendMessages)
  }

  sealed trait Step;
  object Step {
    case class Expect(
      frontendMessages: Seq[FrontendMessage],
      backendMessages: Seq[BackendMessage],
    ) extends Step;

    case class Write(
      frontendMessages: Seq[FrontendMessage],
    ) extends Step;

    case class Read(
      backendMessages: Seq[BackendMessage],
    ) extends Step;

    case class Suspend(
      tag: String,
    ) extends Step;
  }
}
