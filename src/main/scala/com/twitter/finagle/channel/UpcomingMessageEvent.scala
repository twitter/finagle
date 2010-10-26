  //package com.twitter.finagle.channel
  // 
  //import org.jboss.netty.channel._
  //import com.twitter.finagle.util.Conversions._
  // 
  //abstract sealed class BrokeredMessage
  //case class ContinuingBrokeredMessage(content: AnyRef, next: UpcomingMessageEvent)
  //  extends BrokeredMessage
  //case class FinalBrokeredMessage(content: AnyRef) extends BrokeredMessage
  //case class FailedBrokeredMessage(cause: Throwable) extends BrokeredMessage
  // 
  //class UpcomingMessageEvent(channel: Channel) {
  //  @volatile private var message: Option[BrokeredMessage] = None
  //  val future = Channels.future(channel, true)
  // 
  //  // XXX - share doneFuture
  // 
  //  def apply(f: BrokeredMessage => Unit) {
  //    future { _=> f(message.get) }
  //  }
  // 
  //  def update(message: BrokeredMessage) {
  //    this.message = Some(message)
  // 
  //    message match {
  //      case FinalBrokeredMessage(_) | ContinuingBrokeredMessage(_, _) =>
  //        future.setSuccess()
  //      case FailedBrokeredMessage(cause) =>
  //        future.setFailure(cause)
  //    }
  //  }
  // 
  //  // def whenDone()
  //}
  // 
  //class UpcomingMessageEvent2(channel: Channel, doneFuture: ChannelFuture) extends MessageEvent {
  //  def this(channel: Channel) = this(channel, Channels.future(channel))
  // 
  //  //@volatile private var message_: Option[AnyRef] = None
  // 
  //  @volatile private var message: Option[BrokeredMessage] = None
  //  @volatile private var next: Option[UpcomingMessageEvent] = None
  //  private val future = Channels.future(channel, true)
  // 
  //  // private var message: Option[BrokeredMessage] = None
  // 
  //  def setMessage(message: BrokeredMessage) {
  //    this.message = Some(message)
  //  }
  // 
  //  // def setNextMessage(message: ) = {
  //  //   val next = new UpcomingMessageEvent(channel, doneFuture)
  //  //   this.message = Some(message)
  //  //   this.next = Some(next)
  //  //   future.setSuccess()
  //  //   next
  //  // }
  // 
  //  // def setFinalMessage(message: AnyRef) {
  //  //   this.message = Some(message)
  //  //   future.setSuccess()
  //  //   doneFuture.setSuccess()
  //  // }
  // 
  //  def setFailure(cause: Throwable) {
  //    future.setFailure(cause)
  //    doneFuture.setFailure(cause)
  //  }
  // 
  //  def cancel() {
  //    future.cancel()
  //  }
  // 
  //  // def onMessageReceived(f: MessageEvent)
  // 
  //  def getChannel: Channel = channel
  //  def getFuture: ChannelFuture = future
  //  def getDoneFuture: ChannelFuture = doneFuture
  //  def getMessage = message getOrElse null
  //  def getNext = next
  //  def getRemoteAddress = channel.getRemoteAddress
  //}
  // 
  //object UpcomingMessageEvent {
  //  def successfulEvent(channel: Channel, content: AnyRef) = {
  //    val e = new UpcomingMessageEvent(channel)
  //    e() = FinalBrokeredMessage(content)
  //    e
  //  }
  // 
  //  def failedEvent(channel: Channel, cause: Throwable) = {
  //    val e = new UpcomingMessageEvent(channel)
  //    e() = FailedBrokeredMessage(cause)
  //    e
  //  }
  // 
  //}
