package com.twitter.finagle.thrift

import com.twitter.finagle.{Service, SimpleFilter}
import com.twitter.io.Buf
import com.twitter.util.Future
import org.apache.thrift.{TApplicationException, TException}
import org.apache.thrift.protocol.{TMessage, TMessageType, TProtocolFactory}
import scala.util.control.NoStackTrace

private[finagle] object UncaughtAppExceptionFilter {

  /**
   * Creates a Thrift exception message for the given `exception` and thrift `thriftRequest`
   * message using the given [[org.apache.thrift.protocol.TProtocolFactory]].
   */
  def writeExceptionMessage(
    thriftRequest: Buf,
    throwable: Throwable,
    protocolFactory: TProtocolFactory
  ): Buf = {
    val reqBytes = Buf.ByteArray.Owned.extract(thriftRequest)
    // NB! This is technically incorrect for one-way calls,
    // but we have no way of knowing it here. We may
    // consider simply not supporting one-way calls at all.
    val msg = InputBuffer.readMessageBegin(reqBytes, protocolFactory)
    val name = msg.name

    val buffer = new OutputBuffer(protocolFactory)
    buffer().writeMessageBegin(new TMessage(name, TMessageType.EXCEPTION, msg.seqid))

    // Note: The wire contents of the exception message differ from Apache's Thrift in that here,
    // e.toString is appended to the error message.
    // As it is the only place where the TApplicationException is constructed with this error code
    // and the stack trace is not serialised, it doesn't make any sense to populate the stack
    // trace at all

    val x = new TApplicationException(
      TApplicationException.INTERNAL_ERROR,
      s"Internal error processing $name: '$throwable'"
    ) with NoStackTrace

    x.write(buffer())
    buffer().writeMessageEnd()
    Buf.ByteArray.Owned(buffer.toArray)
  }

}

private[finagle] class UncaughtAppExceptionFilter(protocolFactory: TProtocolFactory)
    extends SimpleFilter[Array[Byte], Array[Byte]] {
  import UncaughtAppExceptionFilter.writeExceptionMessage

  def apply(request: Array[Byte], service: Service[Array[Byte], Array[Byte]]): Future[Array[Byte]] =
    service(request).handle {
      case e if !e.isInstanceOf[TException] =>
        val buf = Buf.ByteArray.Owned(request)
        val msg = writeExceptionMessage(buf, e, protocolFactory)
        Buf.ByteArray.Owned.extract(msg)
    }
}
