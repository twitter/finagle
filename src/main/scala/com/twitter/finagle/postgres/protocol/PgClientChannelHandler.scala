package com.twitter.finagle.postgres.protocol

import org.jboss.netty.channel.{Channels, MessageEvent, ChannelHandlerContext, SimpleChannelHandler}
import org.jboss.netty.buffer.ChannelBuffers
import com.twitter.logging.Logger
import java.util.concurrent.ConcurrentLinkedQueue
import collection.mutable.ListBuffer
import scala.PartialFunction

trait State {self =>
  def accept: PartialFunction[Message, (Option[PgResponse], State)] = Map.empty

  def andThen(f: PartialFunction[PgResponse, State]): State = new State {
    override def accept = {
      self.accept.andThen {
        case (Some(Error(details)), s) => (Some(Error(details)), s)
        case (Some(r), _) => (None, f(r))
        case (None, s) => (None, this)
      }
    }
  }

  def andThenReturn(f: PartialFunction[PgResponse, (Option[PgResponse], State)]): State = new State {
    override def accept = {
      self.accept.andThen {
        case (Some(Error(details)), s) => (Some(Error(details)), s)
        case (Some(r), _) => f(r)
        case (None, s) => (None, this)
      }
    }
  }

  def orElse(other: State) = new State {
    override def accept = self.accept.orElse(other.accept)
  }
}

case object AuthenticationRequired extends State {
  override def accept = {
    case StartupMessage(_, _) => (None, AuthenticationInProgress)
  }
}

case object AuthenticationInProgress extends State {


  override def accept = {
    case PasswordMessage(_) => (None, this)
    case AuthenticationCleartextPassword() => (Some(PasswordRequired(ClearText)), this)
    case AuthenticationMD5Password(salt) => (Some(PasswordRequired(Md5(salt))), this)
    case AuthenticationOk() => (None, AggregatingAuthData(Map(), -1, -1))
    case ErrorResponse(details) => (Some(Error(details)), AuthenticationRequired)
  }
}

case class AggregatingAuthData(statuses: Map[String, String], processId: Int, secretKey: Int) extends State {
  override def accept = {
    case ParameterStatus(name, value) => (None, copy(statuses = statuses + (name -> value)))
    case BackendKeyData(pid, secret) => (None, copy(processId = pid, secretKey = secret))
    case ReadyForQuery(_) => (Some(AuthenticatedResponse(statuses, processId, secretKey)), Connected)
    case ErrorResponse(details) => (Some(Error(details)), AuthenticationRequired)
  }
}

case object Connected extends State {

  override def accept = {
    case Query(_) => (None, new AwaitReadyForQueryWrapper(SimpleQuery.simpleQueryHandler))
    case Parse(_, _, _) => (None, Parsing)
    case Bind(_, _, _, _, _) => (None, Binding)
    case Describe(false, _) => (None, AwaitParamsDescription.andThen {
      case ParamsResponse(types) => AwaitRowDescription.andThenReturn {
        case RowDescriptions(fields) => (Some(Descriptions(types, fields)), Connected)
      }
    })
    case Describe(true, _) => (None, AwaitRowDescription)
    case Execute(_, _) => (None, EmptyQueryResponseHandler.orElse(AwaitUpdateQueryResult).orElse(AggregateRowData()))
    case Sync => (None, AwaitReadyForQuery(ReadyForQueryResponse, Connected))

  }
}

class AwaitReadyForQueryWrapper(underlying: State) extends State {
  override def accept = underlying.accept.andThen {
    case (Some(emit), state) => (None, AwaitReadyForQuery(emit, state))
    case (None, state) => (None, new AwaitReadyForQueryWrapper(state))
  }
}

case class AwaitReadyForQuery(emit: PgResponse, next: State) extends State {
  override def accept = {
    case ReadyForQuery(_) => (Some(emit), next)
    case ErrorResponse(details) => (Some(Error(details)), next)
  }
}

case object AwaitParamsDescription extends State {

  override def accept = {
    case ParameterDescription(types) => (Some(ParamsResponse(types)), Connected)
    case ErrorResponse(details) => (Some(Error(details)), Connected)
  }

}

case object AwaitRowDescription extends State {
  override def accept = {
    case RowDescription(fields) =>
      (Some(RowDescriptions(fields.map(f => Field(f.name, f.fieldFormat, f.dataType)))), Connected)
    case NoData => (Some(RowDescriptions(IndexedSeq())), Connected)
    case ErrorResponse(details) => (Some(Error(details)), Connected)
  }
}

case class AggregateRowData(buff: ListBuffer[DataRow] = ListBuffer()) extends State {
  override def accept = {
    case row: DataRow =>
      buff += row
      (None, this)
    case CommandComplete(_) => (Some(Rows(buff.toList, completed = true)), Connected)
    case PortalSuspended => (Some(Rows(buff.toList, completed = false)), Connected)
    case ErrorResponse(details) => (Some(Error(details)), Connected)
  }

}

case object AwaitUpdateQueryResult extends State {
  override def accept = {
    case CommandComplete(tag) => (Some(parseTag(tag)), Connected)
    case ErrorResponse(details) => (Some(Error(details)), Connected)
  }

  private[this] def parseTag(tag: String): CommandCompleteResponse = {
    if (tag == "CREATE TABLE") {
      CommandCompleteResponse(1)
    } else if (tag == "DROP TABLE") {
      CommandCompleteResponse(1)
    } else {
      val parts = tag.split(" ")

      parts(0) match {
        case "INSERT" => CommandCompleteResponse(parts(2).toInt)
        case "DELETE" => CommandCompleteResponse(parts(1).toInt)
        case "UPDATE" => CommandCompleteResponse(parts(1).toInt)
        case _ => throw new IllegalStateException("Unknown command complete response tag " + tag)
      }
    }
  }


}

case object EmptyQueryResponseHandler extends State {
  override def accept = {
    case EmptyQueryResponse => (Some(SelectResult(IndexedSeq(), List())), Connected) // TODO exception???ª
    case ErrorResponse(details) => (Some(Error(details)), Connected)
  }

}

object SimpleQuery {
  private[this] val selectQuerySequence: State = AwaitRowDescription.andThen {
    case RowDescriptions(fields) => AggregateRowData().andThenReturn {
      case Rows(data, true) => (Some(SelectResult(fields, data)), Connected)
    }
  }

  val simpleQueryHandler = EmptyQueryResponseHandler.orElse(AwaitUpdateQueryResult).orElse(selectQuerySequence)
}

case object Parsing extends State {
  override def accept = {
    case ParseComplete => (Some(ParseCompletedResponse), Connected)
    case ErrorResponse(details) => (Some(Error(details)), Connected)
  }
}

case object Binding extends State {
  override def accept = {
    case BindComplete => (Some(BindCompletedResponse), Connected)
    case ErrorResponse(details) => (Some(Error(details)), Connected)
  }
}

class Connection(@volatile private[this] var state: State = AuthenticationRequired,
                 @volatile private[this] var busy: Boolean = false) {
  private[this] val logger = Logger(getClass.getName)

  private[this] val frontendQueue = new ConcurrentLinkedQueue[FrontendMessage]

  def send(msg: FrontendMessage) {
    logger.ifDebug("Frontend message accepted " + msg)
    frontendQueue.offer(msg)
  }

  def receive(msg: BackendMessage): Option[PgResponse] = {
    logger.ifDebug("Backend message received " + msg)

    if (!busy) {
      val pending = frontendQueue.peek
      logger.ifDebug("Frontend message is pending " + pending)
      state = state.accept(pending)._2
      busy = true
    }

    val (result, newState) = state.accept(msg)
    state = newState
    if (result.isDefined) {
      busy = false
      frontendQueue.poll
    }
    logger.ifDebug("Result " + result + " new state " + state)
    result
  }

}


class PgClientChannelHandler extends SimpleChannelHandler {

  private[this] val logger = Logger(getClass.getName)

  private[this] val connection = new Connection()

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    val message = e.getMessage

    message match {
      case msg: BackendMessage =>
        connection.receive(msg).map {
          Channels.fireMessageReceived(ctx, _)
        }

      case unsupported =>
        logger.warning("Only backend message is supported...")
        Channels.disconnect(ctx.getChannel)
    }

  }

  override def writeRequested(ctx: ChannelHandlerContext, event: MessageEvent) = {
    logger.ifDebug("Encoding message " + event.getMessage)

    val buf = event.getMessage match {
      case PgRequest(msg, flush) =>
        // TODO this could be extracted as a separate handler
        val packet = msg.asPacket()
        val c = ChannelBuffers.dynamicBuffer()
        c.writeBytes(packet.encode)
        if (flush) {
          c.writeBytes(Flush.asPacket.encode)
        }
        connection.send(msg)
        c
      case _ =>
        logger.ifDebug("Cannot convert message... Skipping")
        event.getMessage
    }
    Channels.write(ctx, event.getFuture, buf, event.getRemoteAddress)
  }

}

class UnsupportedMessage(message: String) extends Exception(message)
