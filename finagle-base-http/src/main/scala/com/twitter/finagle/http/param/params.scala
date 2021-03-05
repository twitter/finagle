package com.twitter.finagle.http.param

import com.twitter.conversions.StorageUnitOps._
import com.twitter.finagle.Stack
import com.twitter.util.StorageUnit

/**
 * automatically send 100-CONTINUE responses to requests which set
 * the 'Expect: 100-Continue' header. See longer note on
 * `com.twitter.finagle.Http.Server#withNoAutomaticContinue`
 */
case class AutomaticContinue(enabled: Boolean)
object AutomaticContinue {
  implicit val automaticContinue: Stack.Param[AutomaticContinue] =
    Stack.Param(AutomaticContinue(true))
}

/**
 * the maximum size of all headers.
 */
case class MaxHeaderSize(size: StorageUnit)
object MaxHeaderSize {
  implicit val maxHeaderSizeParam: Stack.Param[MaxHeaderSize] =
    Stack.Param(MaxHeaderSize(8.kilobytes))
}

/**
 * the maximum size of the initial line.
 */
case class MaxInitialLineSize(size: StorageUnit)
object MaxInitialLineSize {
  implicit val maxInitialLineSizeParam: Stack.Param[MaxInitialLineSize] =
    Stack.Param(MaxInitialLineSize(4.kilobytes))
}

/**
 * The maximum size of an inbound HTTP request that this
 * Finagle server can receive from a client.
 *
 * @note This param only applies to Finagle HTTP servers,
 * and not to Finagle HTTP clients.
 */
case class MaxRequestSize(size: StorageUnit) {
  require(size < 2.gigabytes, s"MaxRequestSize should be less than 2 Gb, but was $size")
}
object MaxRequestSize {
  implicit val maxRequestSizeParam: Stack.Param[MaxRequestSize] =
    Stack.Param(MaxRequestSize(5.megabytes))
}

/**
 * The maximum size of an inbound HTTP response that this
 * Finagle client can receive from a server.
 *
 * @note This param only applies to Finagle HTTP clients,
 * and not to Finagle HTTP servers.
 */
case class MaxResponseSize(size: StorageUnit) {
  require(size < 2.gigabytes, s"MaxResponseSize should be less than 2 Gb, but was $size")
}
object MaxResponseSize {
  implicit val maxResponseSizeParam: Stack.Param[MaxResponseSize] =
    Stack.Param(MaxResponseSize(5.megabytes))
}

sealed abstract class Streaming private {
  def enabled: Boolean
  final def disabled: Boolean = !enabled
}
object Streaming {

  private[finagle] case object Disabled extends Streaming {
    def enabled: Boolean = false
  }

  private[finagle] final case class Enabled(fixedLengthStreamedAfter: StorageUnit)
      extends Streaming {
    def enabled: Boolean = true
  }

  implicit val streamingParam: Stack.Param[Streaming] =
    Stack.Param(Disabled)

  def apply(enabled: Boolean): Streaming =
    if (enabled) Enabled(5.megabytes)
    else Disabled

  def apply(fixedLengthStreamedAfter: StorageUnit): Streaming =
    Enabled(fixedLengthStreamedAfter)
}

case class FixedLengthStreamedAfter(size: StorageUnit)
object FixedLengthStreamedAfter {
  implicit val fixedLengthStreamedAfter: Stack.Param[FixedLengthStreamedAfter] =
    Stack.Param(FixedLengthStreamedAfter(5.megabytes))
}

case class Decompression(enabled: Boolean)
object Decompression extends {
  implicit val decompressionParam: Stack.Param[Decompression] =
    Stack.Param(Decompression(enabled = true))
}

case class CompressionLevel(level: Int)
object CompressionLevel {
  implicit val compressionLevelParam: Stack.Param[CompressionLevel] =
    Stack.Param(CompressionLevel(-1))
}

/**
 * Case class to configure jaas params
 *
 * @param principal The name of the principal that should be used
 * @param keyTab Set this to the file name of the keytab to get principal's secret key
 * @param useKeyTab Set this to true if you want the module to get the principal's key from the the keytab
 * @param storeKey Set this to true to if you want the keytab or the principal's key to be stored in
 *                 the Subject's private credentials
 * @param refreshKrb5Config Set this to true, if you want the configuration to be refreshed before
 *                          the login method is called
 * @param debug Output debug messages
 * @param doNotPrompt Set this to true if you do not want to be prompted for the password if
 *                    credentials can not be obtained from the cache, the keytab, or through shared state
 * @see [[https://docs.oracle.com/javase/7/docs/jre/api/security/jaas/spec/com/sun/security/auth/module/Krb5LoginModule.html]]
 */
case class KerberosConfiguration(
  principal: Option[String] = None,
  keyTab: Option[String] = None,
  useKeyTab: Boolean = true,
  storeKey: Boolean = true,
  refreshKrb5Config: Boolean = true,
  debug: Boolean = false,
  doNotPrompt: Boolean = true,
  authEnabled: Boolean = true)
case class Kerberos(kerberosConfiguration: KerberosConfiguration) {
  def mk(): (Kerberos, Stack.Param[Kerberos]) =
    (this, Kerberos.kerberosParam)
}
object Kerberos {
  implicit val kerberosParam: Stack.Param[Kerberos] =
    Stack.Param(Kerberos(KerberosConfiguration()))
}
