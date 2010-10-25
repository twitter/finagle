package com.twitter.finagle.channel

import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicInteger
import java.nio.channels.NotYetConnectedException

import org.jboss.netty.channel._
import org.jboss.netty.channel.local.LocalAddress

import com.twitter.finagle.util.{Ok, Error}
import com.twitter.finagle.util.Conversions._

class TooManyDicksOnTheDanceFloorException extends Exception

class BrokeredChannel(
  factory: BrokeredChannelFactory,
  pipeline: ChannelPipeline,
  sink: ChannelSink)
  extends AbstractChannel(null/* parent */, factory, pipeline, sink)
{
  val config = new DefaultChannelConfig
  private val localAddress = new LocalAddress(LocalAddress.EPHEMERAL)
  @volatile private var broker: Option[Broker] = None
  private var waitingForResponse: Option[UpcomingMessageEvent] = None

  private val nwaiters = new AtomicInteger(0)
  private val executionQueue = new LinkedBlockingQueue[Function0[Unit]]

  def serialized[T](f: T => Unit): T => Unit = { x => serialized { f(x) } }
  def serialized(f: => Unit) {
    executionQueue offer { () => f }

    if (nwaiters.getAndIncrement() == 0) {
      do {
        executionQueue.poll()()
      } while (nwaiters.decrementAndGet() > 0)
    }
  }

  protected[channel] def realConnect(broker: Broker, future: ChannelFuture) = serialized {
    this.broker = Some(broker)
    future.setSuccess()
    Channels.fireChannelConnected(this, broker)
    Channels.fireChannelBound(this, broker)
  }

  protected[channel] def realClose(future: ChannelFuture) = serialized {
    for (response <- waitingForResponse)
      response.cancel()

    waitingForResponse = None

    setClosed()
    Channels.fireChannelClosed(this)
    if (broker.isDefined) {
      Channels.fireChannelDisconnected(this)
      Channels.fireChannelUnbound(this)
      broker = None
    }
  }

  protected[channel] def realWrite(e: MessageEvent): Unit = serialized {
    broker match {
      case Some(broker) if !waitingForResponse.isDefined =>
        val responseEvent = broker.dispatch(e)
        waitingForResponse = Some(responseEvent)

        e.getFuture() { serialized {
          case Ok(_) if this.isOpen =>
            Channels.fireWriteComplete(this, 1)
          case Error(cause) if isOpen =>
            Channels.fireExceptionCaught(this, cause)
          case _ => ()
        }}

        responseEvent.getFuture() { state =>
          serialized {
            state match {
              case Ok(_) if this.isOpen =>
                Channels.fireMessageReceived(this, responseEvent.getMessage)
              case Error(cause) if isOpen =>
                Channels.fireExceptionCaught(this, cause)
              case _ => ()
            }
            waitingForResponse = None
          }
        }


      case Some(_) if waitingForResponse.isDefined =>
        Channels.fireExceptionCaught(this, new TooManyDicksOnTheDanceFloorException)

      case _ =>
        e.getFuture.setFailure(new NotYetConnectedException)
    }
    ()
  }

  def getRemoteAddress = broker.getOrElse(null)
  def getLocalAddress = if (broker.isDefined) localAddress else null

  def isConnected = broker.isDefined
  def isBound = broker.isDefined
  def getConfig = config

  private sealed abstract class State
  private case object Idle extends State
  private case object PreparingRequest extends State
  private case class WaitingForResponse(responseEvent: UpcomingMessageEvent) extends State
}
