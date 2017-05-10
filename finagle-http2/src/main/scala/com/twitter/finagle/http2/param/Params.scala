package com.twitter.finagle.http2.param

import com.twitter.finagle.Stack
import com.twitter.util.StorageUnit
import io.netty.handler.codec.http2.Http2HeadersEncoder

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
 *  A class for configuring overrides to the default pushEnabled setting.
 */
case class PushEnabled(pushEnabled: Option[Boolean]) {
  def mk(): (PushEnabled, Stack.Param[PushEnabled]) =
    (this, PushEnabled.param)
}

object PushEnabled {
  implicit val param = Stack.Param(PushEnabled(None))
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
case class MaxHeaderListSize(maxHeaderListSize: Option[StorageUnit]) {
  def mk(): (MaxHeaderListSize, Stack.Param[MaxHeaderListSize]) =
    (this, MaxHeaderListSize.param)
}

object MaxHeaderListSize {
  implicit val param = Stack.Param(MaxHeaderListSize(None))
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
 * A class for configuring the http/2 encoder to ignore MaxHeaderListSize.
 *
 * This is useful when creating clients for testing the behavior of a server.
 */
case class HeaderSensitivity(sensitivityDetector: (CharSequence, CharSequence) => Boolean) {
  def mk(): (HeaderSensitivity, Stack.Param[HeaderSensitivity]) =
    (this, HeaderSensitivity.param)
}

object HeaderSensitivity {
  private[this] val DefaultSensitivityDetector: (CharSequence, CharSequence) => Boolean =
    new Function2[CharSequence, CharSequence, Boolean] {
      def apply(name: CharSequence, value: CharSequence): Boolean = {
        Http2HeadersEncoder.NEVER_SENSITIVE.isSensitive(name, value)
      }

      override def toString(): String = "NEVER_SENSITIVE"
    }

  implicit val param = Stack.Param(HeaderSensitivity(DefaultSensitivityDetector))
}
