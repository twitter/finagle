package com.twitter.finagle.channel

import org.jboss.netty.channel._
import java.util.concurrent.atomic.AtomicInteger
import java.util.Queue
import scala._

import com.twitter.concurrent.Serialized

private[finagle] case class Job(ctx: ChannelHandlerContext, e: MessageEvent) extends Serialized {
  @volatile private var _isAlive = true
  @volatile private var _isStarted = false

  def apply() {
    _isStarted = true
    Channels.fireMessageReceived(ctx, e, e.getRemoteAddress)
  }

  def isAlive = _isAlive
  def isStarted = _isStarted
  def markDead() { _isAlive = false }
}

class QueueingChannelHandler(concurrencyLevel: Int, queue: Queue[Job])
  extends SimpleChannelHandler
{
  private val outstandingRequests = new AtomicInteger(0)

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    val job = Job(ctx, e)
    queue.offer(job)
    ctx.setAttachment(job)
    performNextJob()
  }


  override def writeRequested(ctx: ChannelHandlerContext, e: MessageEvent) {
    outstandingRequests.getAndDecrement()
    super.writeRequested(ctx, e)
    performNextJob()
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
    cancelJob(ctx)
    super.exceptionCaught(ctx, e)
  }

  override def channelClosed(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    cancelJob(ctx)
    super.channelClosed(ctx, e)
  }

  override def closeRequested(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    cancelJob(ctx)
    super.closeRequested(ctx, e)
  }

  private def cancelJob(ctx: ChannelHandlerContext): Unit = {
    val job = ctx.getAttachment().asInstanceOf[Job]
    if (job ne null) {
      job.serialized {
        if (job.isStarted)
          outstandingRequests.getAndDecrement()
        else
          job.markDead()
      }
    }
    performNextJob()
  }

  private def performNextJob() {
    val items = outstandingRequests.getAndIncrement()
    if (items < concurrencyLevel) {
      val job = getNextJob()
      job foreach(_.apply())
    } else
      outstandingRequests.decrementAndGet()
  }

  private def getNextJob() = {
    var job: Job = null
    do {
      job = queue.poll()
    } while ((job ne null) && !job.isAlive)
    Option(job)
  }
}


