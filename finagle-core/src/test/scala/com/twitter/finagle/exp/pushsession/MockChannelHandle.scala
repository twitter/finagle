package com.twitter.finagle.exp.pushsession

import com.twitter.finagle.Status
import com.twitter.util.{Future, Promise, Time, Try}
import java.net.SocketAddress
import java.security.cert.Certificate
import java.util
import java.util.concurrent.Executor
import scala.collection.mutable

class MockChannelHandle[In, Out](var currentSession: PushSession[In, Out]) extends PushChannelHandle[In, Out] {
  def this() = this(null)

  sealed trait SendCommand {
    def msgs: Iterable[Out]
  }

  case class SendAndForgetMany(msgs: Iterable[Out]) extends SendCommand
  case class SendMany(msgs: Iterable[Out], completion: Try[Unit] => Unit) extends SendCommand

  case class SendAndForgetOne(msg: Out) extends SendCommand {
    def msgs: Iterable[Out] = Seq(msg)
  }

  case class SendOne(msg: Out, completion: Try[Unit] => Unit) extends SendCommand {
    def msgs: Iterable[Out] = Seq(msg)
  }

  val onClosePromise: Promise[Unit] = Promise[Unit]()

  var closedCalled: Boolean = false
  var status: Status = Status.Open

  val pendingWrites = new mutable.Queue[SendCommand]

  val serialExecutor: DeferredExecutor = new DeferredExecutor

  def registerSession(newSession: PushSession[In, Out]): Unit = {
    currentSession = newSession
  }

  def send(messages: Iterable[Out])
    (onComplete: (Try[Unit]) => Unit): Unit = {
    pendingWrites += SendMany(messages, onComplete)
  }

  def send(message: Out)
    (onComplete: (Try[Unit]) => Unit): Unit = {
    pendingWrites += SendOne(message, onComplete)
  }

  def sendAndForget(message: Out): Unit = {
    pendingWrites += SendAndForgetOne(message)
  }

  def sendAndForget(messages: Iterable[Out]): Unit = {
    pendingWrites += SendAndForgetMany(messages)
  }

  def peerCertificate: Option[Certificate] = None

  def remoteAddress: SocketAddress = ???

  def localAddress: SocketAddress = ???

  def onClose: Future[Unit] = onClosePromise

  def close(deadline: Time): Future[Unit] = {
    closedCalled = true
    onClose
  }
}

class DeferredExecutor extends Executor {
  private[this] val queue = new util.ArrayDeque[Runnable]()
  def execute(command: Runnable): Unit = queue.add(command)

  /** Execute all pending messages, including those queued during a previous execution */
  def executeAll(): Unit = {
    while (!queue.isEmpty) {
      queue.poll().run()
    }
  }
}
