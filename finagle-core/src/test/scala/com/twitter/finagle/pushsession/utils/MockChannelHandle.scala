package com.twitter.finagle.pushsession.utils

import com.twitter.finagle.Status
import com.twitter.finagle.pushsession.{PushChannelHandle, PushSession}
import com.twitter.finagle.ssl.session.{NullSslSessionInfo, SslSessionInfo}
import com.twitter.util.{Future, Promise, Return, Time, Try}
import java.net.{InetSocketAddress, SocketAddress}
import scala.collection.mutable

class MockChannelHandle[In, Out](var currentSession: PushSession[In, Out])
    extends PushChannelHandle[In, Out] {
  def this() = this(null)

  sealed trait SendCommand {
    def msgs: Vector[Out]
    def completeWith(result: Try[Unit]): Unit
    def completeSuccess(): Unit = completeWith(Return.Unit)
  }

  case class SendAndForgetMany(msgs: Vector[Out]) extends SendCommand {
    def completeWith(result: Try[Unit]): Unit = ()
  }
  case class SendMany(msgs: Vector[Out], completion: Try[Unit] => Unit) extends SendCommand {
    def completeWith(result: Try[Unit]): Unit = completion(result)
  }

  case class SendAndForgetOne(msg: Out) extends SendCommand {
    def msgs: Vector[Out] = Vector(msg)
    def completeWith(result: Try[Unit]): Unit = ()
  }

  case class SendOne(msg: Out, completion: Try[Unit] => Unit) extends SendCommand {
    def msgs: Vector[Out] = Vector(msg)
    def completeWith(result: Try[Unit]): Unit = completion(result)
  }

  val onClosePromise: Promise[Unit] = Promise[Unit]()

  var closedCalled: Boolean = false
  var status: Status = Status.Open

  val pendingWrites = new mutable.Queue[SendCommand]

  val serialExecutor: DeferredExecutor = new DeferredExecutor

  def dequeAndCompleteWrite(): Vector[Out] = {
    val write = pendingWrites.dequeue()
    write.completeSuccess()
    write.msgs
  }

  def registerSession(newSession: PushSession[In, Out]): Unit = {
    currentSession = newSession
  }

  def send(messages: Iterable[Out])(onComplete: (Try[Unit]) => Unit): Unit = {
    pendingWrites += SendMany(messages.toVector, onComplete)
  }

  def send(message: Out)(onComplete: (Try[Unit]) => Unit): Unit = {
    pendingWrites += SendOne(message, onComplete)
  }

  def sendAndForget(message: Out): Unit = {
    pendingWrites += SendAndForgetOne(message)
  }

  def sendAndForget(messages: Iterable[Out]): Unit = {
    pendingWrites += SendAndForgetMany(messages.toVector)
  }

  def sslSessionInfo: SslSessionInfo = NullSslSessionInfo

  val remoteAddress: SocketAddress = new InetSocketAddress(8888)

  val localAddress: SocketAddress = new InetSocketAddress(8889)

  def onClose: Future[Unit] = onClosePromise

  def close(deadline: Time): Future[Unit] = {
    closedCalled = true
    onClose
  }
}
