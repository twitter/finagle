package com.twitter.finagle.http2.param

import com.twitter.conversions.StorageUnitOps._
import com.twitter.finagle.Stack
import com.twitter.util.StorageUnit
import io.netty.handler.codec.http2.Http2MultiplexHandler

/**
 * A class eligible for configuring whether to use the http/2 "prior knowledge"
 * protocol or not.
 *
 * Note that both client and server must be configured for prior knowledge.
 */
case class PriorKnowledge(enabled: Boolean) {
  def mk(): (PriorKnowledge, Stack.Param[PriorKnowledge]) =
    (this, PriorKnowledge.param)
}

object PriorKnowledge {
  implicit val param = Stack.Param(PriorKnowledge(false))
}

/**
 * A class for configuring overrides to the default headerTableSize setting.
 */
case class HeaderTableSize(headerTableSize: Option[StorageUnit]) {
  def mk(): (HeaderTableSize, Stack.Param[HeaderTableSize]) =
    (this, HeaderTableSize.param)
}

object HeaderTableSize {
  implicit val param = Stack.Param(HeaderTableSize(None))
}

/**
 * A class for configuring overrides to the default pushEnabled setting.
 *
 * Marked as private because finagle doesn't support it yet.  Right now
 * the default of, "Do not use push promises" is the only supported mode.
 */
private[http2] case class PushEnabled(pushEnabled: Option[Boolean]) {
  def mk(): (PushEnabled, Stack.Param[PushEnabled]) =
    (this, PushEnabled.param)
}

private[http2] object PushEnabled {
  implicit val param = Stack.Param(PushEnabled(Some(false)))
}

/**
 * A class for configuring overrides to the default maxConcurrentStreams setting.
 */
case class MaxConcurrentStreams(maxConcurrentStreams: Option[Long]) {
  def mk(): (MaxConcurrentStreams, Stack.Param[MaxConcurrentStreams]) =
    (this, MaxConcurrentStreams.param)
}

object MaxConcurrentStreams {
  implicit val param = Stack.Param(MaxConcurrentStreams(None))
}

/**
 * A class for configuring overrides to the default initialWindowSize setting.
 */
case class InitialWindowSize(initialWindowSize: Option[StorageUnit]) {
  def mk(): (InitialWindowSize, Stack.Param[InitialWindowSize]) =
    (this, InitialWindowSize.param)
}

object InitialWindowSize {
  implicit val param = Stack.Param(InitialWindowSize(None))
}

/**
 * A class for configuring overrides to the default `encoderEnforceMaxConcurrentStreams` setting. If
 * enabled the encoder will queue frames if the maximum number of concurrent streams would otherwise
 * be exceeded.
 */
final case class EnforceMaxConcurrentStreams(enabled: Boolean) {
  def mk(): (EnforceMaxConcurrentStreams, Stack.Param[EnforceMaxConcurrentStreams]) =
    (this, EnforceMaxConcurrentStreams.param)
}

object EnforceMaxConcurrentStreams {
  implicit val param = Stack.Param(EnforceMaxConcurrentStreams(enabled = false))
}

/**
 * A class for configuring overrides to the default maxFrameSize setting.
 */
case class MaxFrameSize(maxFrameSize: Option[StorageUnit]) {
  def mk(): (MaxFrameSize, Stack.Param[MaxFrameSize]) =
    (this, MaxFrameSize.param)
}

object MaxFrameSize {
  implicit val param = Stack.Param(MaxFrameSize(None))
}

/**
 * A class for configuring overrides to the default maxHeaderListSize setting.
 */
case class MaxHeaderListSize(maxHeaderListSize: StorageUnit) {
  def mk(): (MaxHeaderListSize, Stack.Param[MaxHeaderListSize]) =
    (this, MaxHeaderListSize.param)
}

object MaxHeaderListSize {
  // TODO: revert to 8.kilobytes after we resolve https://github.com/netty/netty/issues/7511
  // Netty is double counting header names right now.
  implicit val param = Stack.Param(MaxHeaderListSize(16.kilobytes))
}

/**
 * A class for configuring the http/2 encoder to ignore MaxHeaderListSize.
 *
 * This is useful when creating clients for testing the behavior of a server.
 */
case class EncoderIgnoreMaxHeaderListSize(ignoreMaxHeaderListSize: Boolean) {
  def mk(): (EncoderIgnoreMaxHeaderListSize, Stack.Param[EncoderIgnoreMaxHeaderListSize]) =
    (this, EncoderIgnoreMaxHeaderListSize.param)
}

object EncoderIgnoreMaxHeaderListSize {
  implicit val param = Stack.Param(EncoderIgnoreMaxHeaderListSize(false))
}

/**
 * A class for configuring the http/2 encoder to mark a header entry as
 * sensitive or not.
 *
 * Entries marked as sensitive will never be cached, and will be encoded as
 * literals, either with a static huffman encoding or in cleartext.
 */
case class HeaderSensitivity(sensitivityDetector: (CharSequence, CharSequence) => Boolean) {
  def mk(): (HeaderSensitivity, Stack.Param[HeaderSensitivity]) =
    (this, HeaderSensitivity.param)
}

object HeaderSensitivity {
  val NeverSensitive: (CharSequence, CharSequence) => Boolean =
    new Function2[CharSequence, CharSequence, Boolean] {
      def apply(name: CharSequence, value: CharSequence): Boolean = false

      override def toString(): String = "NEVER_SENSITIVE"
    }
  val AlwaysSensitive: (CharSequence, CharSequence) => Boolean =
    new Function2[CharSequence, CharSequence, Boolean] {
      def apply(name: CharSequence, value: CharSequence): Boolean = true

      override def toString(): String = "ALWAYS_SENSITIVE"
    }

  implicit val param = Stack.Param(HeaderSensitivity(NeverSensitive))
}

/**
 * Whether or not HTTP/2 frame logging is enabled.
 *
 * Defaults to disabled.
 *
 * @see `Enabled` and `Disabled` on companion class for getting instances.
 */
final case class FrameLogging private (enabled: Boolean) {
  def mk(): (FrameLogging, Stack.Param[FrameLogging]) =
    (this, FrameLogging.param)
}

object FrameLogging {

  /**
   * Frame logging is disabled.
   */
  val Disabled: FrameLogging = FrameLogging(false)

  /**
   * Frame logging is enabled.
   *
   * @see [[FrameLoggerNamePrefix]] for further configuration.
   */
  val Enabled: FrameLogging = FrameLogging(true)

  implicit val param: Stack.Param[FrameLogging] = Stack.Param(Disabled)
}

/**
 * The logger name to be used for the root HTTP/2 frame logger. This allows each frame type
 * to be turned on and off by changing the level of prefix.<FRAME_TYPE>, or turning everything
 * on by changing the level of prefix. The HTTP/2 frame logger logs at the level TRACE, so you
 * must set logger to that level to see the frame logs. The prefix if not set defaults to
 * io.netty.handler.codec.http2.Http2MultiplexHandler
 *
 * @param loggerNamePrefix The name of the logger to be used as the root logger name for
 *                         netty HTTP/2 frame logging.
 */
case class FrameLoggerNamePrefix(loggerNamePrefix: String) {
  def mk(): (FrameLoggerNamePrefix, Stack.Param[FrameLoggerNamePrefix]) =
    (this, FrameLoggerNamePrefix.param)
}

object FrameLoggerNamePrefix {
  private[this] val DefaultFrameLoggerPrefix: String = classOf[Http2MultiplexHandler].getName()

  implicit val param = Stack.Param(FrameLoggerNamePrefix(DefaultFrameLoggerPrefix))
}

/**
 * Whether or not [[com.twitter.finagle.http2.transport.common.Http2NackHandler]]
 * is included in channel pipeline, which is responsible for converting NACKs
 * to RST_STREAM frame.
 *
 * Defaults to enabled.
 *
 * @see `Enabled` and `Disabled` on companion class for getting instances.
 */
final case class NackRstFrameHandling private(enabled: Boolean) {
  def mk(): (NackRstFrameHandling, Stack.Param[NackRstFrameHandling]) =
    (this, NackRstFrameHandling.param)
}

object NackRstFrameHandling {

  /**
   * Http2NackHandler is disabled.
   */
  val Disabled: NackRstFrameHandling = NackRstFrameHandling(false)

  /**
   * Http2NackHandler is enabled.
   */
  val Enabled: NackRstFrameHandling = NackRstFrameHandling(true)

  implicit val param: Stack.Param[NackRstFrameHandling] = Stack.Param(Enabled)
}
