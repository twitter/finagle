package com.twitter.finagle.channel

import java.nio.channels.NotYetConnectedException
import org.jboss.netty.channel.local.LocalAddress
import org.jboss.netty.channel._
import com.twitter.finagle.util.Conversions._
import com.twitter.finagle.util.{Ok, Error, Cancelled}
import java.util.concurrent.atomic.AtomicReference

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
  private val currentState = new AtomicReference[State](Idle)

  protected[channel] def realConnect(broker: Broker, future: ChannelFuture) {
    this.broker = Some(broker)
    future.setSuccess()
    Channels.fireChannelConnected(this, broker)
    Channels.fireChannelBound(this, broker)
  }

  protected[channel] def realClose(future: ChannelFuture) {
    currentState.get match {
      case WaitingForResponse(responseEvent) => responseEvent.cancel()
      case _ =>
    }

    setClosed()
    Channels.fireChannelClosed(this)
    if (broker.isDefined) {
      Channels.fireChannelDisconnected(this)
      Channels.fireChannelUnbound(this)
      broker = None
    }
  }

  protected[channel] def realWrite(e: MessageEvent) {
    broker match {
      case Some(broker) =>
        if (currentState.compareAndSet(Idle, PreparingRequest())) {
          val responseEvent = broker.dispatch(e)
          currentState.set(WaitingForResponse(responseEvent))
          e.getFuture() {
            case Ok(_) if (isOpen) =>
              Channels.fireWriteComplete(this, 1)
            case Error(cause) if (isOpen) =>
                Channels.fireExceptionCaught(this, cause)
            case _ => ()
          }

          responseEvent.getFuture() { state =>
            state match {
              case Ok(_) if (isOpen) =>
                Channels.fireMessageReceived(this, responseEvent.getMessage)
              case Error(cause) if (isOpen) =>
                Channels.fireExceptionCaught(this, cause)
              case _ => ()
            }
            currentState.set(Idle)
          }

        } else {
          Channels.fireExceptionCaught(this, new TooManyDicksOnTheDanceFloorException)
        }

      case None =>
        e.getFuture.setFailure(new NotYetConnectedException)
    }
  }

  def getRemoteAddress = broker.getOrElse(null)
  def getLocalAddress = if (broker.isDefined) localAddress else null

  def isConnected = broker.isDefined
  def isBound = broker.isDefined
  def getConfig = config

  private abstract class State
  private object Idle extends State
  private case class PreparingRequest() extends State
  private case class WaitingForResponse(responseEvent: UpcomingMessageEvent) extends State
}
