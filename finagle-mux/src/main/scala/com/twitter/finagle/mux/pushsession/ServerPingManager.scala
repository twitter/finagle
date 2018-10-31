package com.twitter.finagle.mux.pushsession

import com.twitter.finagle.mux.transport.Message
import com.twitter.finagle.mux.transport.Message.Tags

/**
 * Manage the ping behavior of the mux server session.
 *
 * @note This expects to always be called from within the serial executor.
 */
trait ServerPingManager {
  def pingReceived(tag: Int): Unit
}

object ServerPingManager {

  /** Generate a default `ServerPingManager`. */
  def default(writer: MessageWriter): ServerPingManager =
    new ServerPingManager {
      def pingReceived(tag: Int): Unit = {
        val response =
          if (tag == Tags.PingTag) Message.PreEncoded.Rping
          else Message.Rping(tag)
        writer.write(response)
      }
    }
}
