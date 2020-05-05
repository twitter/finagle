package com.twitter.finagle.ssl

import com.twitter.finagle.Stack
import com.twitter.io.Buf

private[finagle] object TlsSnooping {

  /**
   * Definition of a TLS snooper
   *
   * The snooping function will be provided the head of the TCP stream with which
   * to determine whether TLS is enabled (or not). If more data is required it may
   * return `TlsDetection.NeedMoreData` to request more data be accumulated and to
   * try again from the beginning.
   */
  type Snooper = Buf => DetectionResult

  /**
   * Default detector implementation that will look for the TLS 1.x record
   * format and expect that the first record is of the type client handshake
   */
  val Tls1XDetection: Snooper = Snoopers.tls12Detector(_)

  case class Param(snooper: Snooper)

  object Param {
    implicit val param = Stack.Param(Param(Tls1XDetection))
  }

  /** Representation of the snooper results based on the current data received */
  sealed abstract class DetectionResult private ()

  object DetectionResult {

    /** The snooper has determined that this connection is cleartext */
    case object Cleartext extends DetectionResult

    /** The snooper has determined that this connection is secure */
    case object Secure extends DetectionResult

    /** The snooper needs more data before making a determination */
    case object NeedMoreData extends DetectionResult
  }
}
