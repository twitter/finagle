/** Copyright 2011 Twitter, Inc. */

package com.twitter.finagle.stream

import org.jboss.netty.handler.codec.http.DefaultHttpChunkTrailer

class SimpleHttpTrailer extends DefaultHttpChunkTrailer {

  /**
   * This is the last empty data piece that we will write out but unfortunately we still have to claim
   * it is not so that Netty will encode it and call our future callback so that we can close
   * connection
   */
  override def isLast(): Boolean = false

}
