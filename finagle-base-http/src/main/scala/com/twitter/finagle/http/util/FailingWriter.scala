package com.twitter.finagle.http.util

import com.twitter.finagle.http.Chunk
import com.twitter.io.{StreamTermination, Writer}
import com.twitter.util.{Future, Time}

/**
 * A [[Writer]] whose each operation fails with [[UnsupportedOperationException]].
 */
private[finagle] object FailingWriter extends Writer[Chunk] {
  def fail(cause: Throwable): Unit = ()

  def write(element: Chunk): Future[Unit] =
    Future.exception(new UnsupportedOperationException("FailingWriter"))

  def close(deadline: Time): Future[Unit] =
    Future.exception(new UnsupportedOperationException("FailingWriter"))

  def onClose: Future[StreamTermination] =
    Future.exception(new UnsupportedOperationException("FailingWriter"))
}
