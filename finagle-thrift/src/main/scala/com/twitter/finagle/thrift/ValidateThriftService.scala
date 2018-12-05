package com.twitter.finagle.thrift

import com.twitter.finagle.{Status, ServiceProxy, Service, WriteException, ServiceException}
import java.util.logging.{Logger, Level}
import org.apache.thrift.TApplicationException
import org.apache.thrift.protocol.{TProtocolFactory, TMessageType}
import org.apache.thrift.transport.TMemoryInputTransport
import com.twitter.util.{Future, Return}

/**
 * Indicates that the connection on which a Thrift request was issued
 * is invalid, where "validity" is determined by
 * [[com.twitter.finagle.thrift.ValidateThriftService]].
 */
case class InvalidThriftConnectionException() extends ServiceException {
  override def getMessage: String = "the thrift connection was invalidated"
}

/**
 * A filter that invalidates a connection if it suffers from an
 * irrecoverable application exception.
 *
 * Amazingly, an Apache Thrift server will leave a connection in a
 * bad state without closing it, and furthermore only expose such
 * errors as an "application" exception.
 *
 * All we can do is sigh, pinch our noses, and apply
 * `ValidateThriftService`.
 */
class ValidateThriftService(
  self: Service[ThriftClientRequest, Array[Byte]],
  protocolFactory: TProtocolFactory)
    extends ServiceProxy[ThriftClientRequest, Array[Byte]](self) {
  @volatile private[this] var isValid = true

  override def apply(req: ThriftClientRequest): Future[Array[Byte]] =
    if (!isValid) Future.exception(WriteException(InvalidThriftConnectionException()))
    else
      self(req).respond {
        case Return(bytes) =>
          if (!req.oneway && !isResponseValid(bytes)) {
            isValid = false
            Logger
              .getLogger("finagle-thrift")
              .log(Level.WARNING, "Thrift connection was invalidated!")
          }
        case _ =>
      }

  override def status: Status =
    if (!isValid) Status.Closed
    else self.status

  private def isResponseValid(bytes: Array[Byte]) =
    try {
      val memoryTransport = new TMemoryInputTransport(bytes)
      val iprot = protocolFactory.getProtocol(memoryTransport)
      val reply = iprot.readMessageBegin()
      reply.`type` != TMessageType.EXCEPTION || {
        val exc = TApplicationException.readFrom(iprot)
        iprot.readMessageEnd()
        exc.getType == TApplicationException.INTERNAL_ERROR ||
        exc.getType == TApplicationException.UNKNOWN_METHOD
      }
    } catch {
      case exc: Throwable =>
        Logger
          .getLogger("finagle-thrift")
          .log(Level.WARNING, "Exception while validating connection", exc)
        false
    }
}
