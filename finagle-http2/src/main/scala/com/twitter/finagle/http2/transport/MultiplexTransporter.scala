package com.twitter.finagle.http2.transport

import com.twitter.finagle.Status
import com.twitter.finagle.client.Transporter
import com.twitter.util.Closable

/** Additional behavior carried by HTTP/2 transporters to deal with multiplexing behavior */
private[finagle] trait MultiplexTransporter extends Closable { self: Transporter[_, _, _] =>

  /** Status of the underlying session */
  def sessionStatus: Status
}
