package com.twitter.finagle.smtp

import com.twitter.finagle.dispatch.GenSerialClientDispatcher
import com.twitter.logging.Logger
import com.twitter.finagle.smtp.reply._
import com.twitter.finagle.transport.Transport
import com.twitter.util.{Await, Time, Future, Promise}
/**
 * A ClientDispatcher that implements SMTP client/server protocol.
 */
class SmtpClientDispatcher(trans: Transport[Request, UnspecifiedReply])
  extends GenSerialClientDispatcher[Request, Reply, Request, UnspecifiedReply](trans) {
  import GenSerialClientDispatcher.wrapWriteException
  import ReplyCode._

  /** Logs client requests and server replies. */
  val log = Logger(getClass.getName)

  /**
   * Performs the connection phase. This is done once
   * before any client-server exchange. Upon the connection
   * the server should send greeting. If the greeting is
   * malformed, the service is closed.
   */
  private[this] val connPhase: Future[Unit] = {
    trans.read() flatMap { greet =>
      val p = new Promise[Reply]
      decodeReply(greet, p)
      p flatMap {
        case ServiceReady(_,_) => Future.Done
        case other => Future.exception(InvalidReply(other.toString))
      }
    } onFailure {
      case _ =>  close()
    }
  }

  /**
   * Reads a reply or a sequence of replies (parts of a multiline reply).
   * Every line of a multiline reply is a
   * [[com.twitter.finagle.smtp.reply.NonTerminalLine]]. Once
   * anything else is received, the reply is counted as complete.
   */
  private def readLines: Future[Seq[UnspecifiedReply]] = {
    trans.read() flatMap  {
      case line: NonTerminalLine => readLines map {lines => lines :+ line}
      case other => Future.value(Seq(other))
    }
  }

  /**
   * Constructs a multiline reply from given sequence of replies.
   * If their codes are not matching, an [[com.twitter.finagle.smtp.reply.InvalidReply]]
   * is returned.
   */
  private def multilineReply(replies: Seq[UnspecifiedReply]): UnspecifiedReply = {
    val lns = replies.map(_.info).reverse
    val valid =
      replies.map(_.code).distinct.length == 1 &&
      replies.collectFirst { case InvalidReply(_) => true } .isEmpty

    // Since we changed code in invalidReply, it will be cast correctly
    if (valid)
      new UnspecifiedReply{
        val code = replies.head.code
        val info = lns.head
        override val isMultiline = true
        override val lines = lns
      }
    else
      new InvalidReply(lns.head) {
        override val code = replies.last.code
        override val isMultiline = true
        override val lines = lns
      }
  }

  /**
   * Dispatch and log a request, satisfying Promise `p` with the response;
   * the returned Future is satisfied when the dispatch is complete:
   * only one request is admitted at any given time.
   */
  protected def dispatch(req: Request, p: Promise[Reply]): Future[Unit] = {
    connPhase flatMap { _ =>
      log.trace("client: %s", req.cmd)
      trans.write(req) rescue {
        wrapWriteException
      } flatMap { unit =>
        readLines
      } map {
        case Seq(rp) => decodeReply(rp, p)

        case replies: Seq[UnspecifiedReply] =>
          val rp = multilineReply(replies)
          decodeReply(rp, p)

      }
    } onFailure {
      _ => close()
    }
  }

  /**
   * Constructs a specified [[com.twitter.finagle.smtp.reply.Reply]] judging by the code
   * of a given [[com.twitter.finagle.smtp.reply.UnspecifiedReply]].
   *
   * @param resp The reply to specify
   */
  private def getSpecifiedReply(resp: UnspecifiedReply): Reply = resp match {
    case specified: Reply => specified
    case _: UnspecifiedReply => {
      resp.code match {
        case SYSTEM_STATUS =>
          new SystemStatus(resp.info) {
          override val isMultiline = resp.isMultiline
            override val lines = resp.lines
          }

        case HELP => new Help(resp.info) {
          override val isMultiline = resp.isMultiline
          override val lines = resp.lines
        }

        case SERVICE_READY =>
          val (domain, info) = resp.info span {_ != ' '}
          new ServiceReady(domain, info) {
            override val isMultiline = resp.isMultiline
            override val lines = resp.lines
          }

        case CLOSING_TRANSMISSION =>
          new ClosingTransmission(resp.info) {
            override val isMultiline = resp.isMultiline
            override val lines = resp.lines
          }
        case OK_REPLY => new OK(resp.info) {
          override val isMultiline = resp.isMultiline
          override val lines = resp.lines
        }

        case TEMP_USER_NOT_LOCAL =>
          new TempUserNotLocal(resp.info) {
            override val isMultiline = resp.isMultiline
            override val lines = resp.lines
          }

        case TEMP_USER_NOT_VERIFIED =>
          new TempUserNotVerified(resp.info) {
            override val isMultiline = resp.isMultiline
            override val lines = resp.lines
          }

        case START_INPUT => new StartInput(resp.info) {
          override val isMultiline = resp.isMultiline
          override val lines = resp.lines
        }

        case SERVICE_NOT_AVAILABLE =>
          new ServiceNotAvailable(resp.info) {
            override val isMultiline = resp.isMultiline
            override val lines = resp.lines
          }

        case TEMP_MAILBOX_UNAVAILABLE =>
          new TempMailboxUnavailable(resp.info) {
            override val isMultiline = resp.isMultiline
            override val lines = resp.lines
          }

        case PROCESSING_ERROR =>
          new ProcessingError(resp.info) {
            override val isMultiline = resp.isMultiline
            override val lines = resp.lines
          }

        case TEMP_INSUFFICIENT_STORAGE =>
          new TempInsufficientStorage(resp.info) {
            override val isMultiline = resp.isMultiline
            override val lines = resp.lines
          }

        case PARAMS_ACCOMODATION_ERROR =>
          new ParamsAccommodationError(resp.info) {
            override val isMultiline = resp.isMultiline
            override val lines = resp.lines
          }

        case SYNTAX_ERROR =>
          new SyntaxError(resp.info) {
            override val isMultiline = resp.isMultiline
            override val lines = resp.lines
          }

        case ARGUMENT_SYNTAX_ERROR =>
          new ArgumentSyntaxError(resp.info) {
            override val isMultiline = resp.isMultiline
            override val lines = resp.lines
          }

        case COMMAND_NOT_IMPLEMENTED =>
          new CommandNotImplemented(resp.info) {
            override val isMultiline = resp.isMultiline
            override val lines = resp.lines

          }
        case BAD_COMMAND_SEQUENCE =>
          new BadCommandSequence(resp.info) {
            override val isMultiline = resp.isMultiline
            override val lines = resp.lines
          }

        case PARAMETER_NOT_IMPLEMENTED =>
          new ParameterNotImplemented(resp.info) {
            override val isMultiline = resp.isMultiline
            override val lines = resp.lines
          }

        case MAILBOX_UNAVAILABLE_ERROR =>
          new MailboxUnavailableError(resp.info) {
            override val isMultiline = resp.isMultiline
            override val lines = resp.lines
          }

        case USER_NOT_LOCAL_ERROR =>
          new UserNotLocalError(resp.info) {
            override val isMultiline = resp.isMultiline
            override val lines = resp.lines
          }

        case INSUFFICIENT_STORAGE_ERROR =>
          new InsufficientStorageError(resp.info) {
            override val isMultiline = resp.isMultiline
            override val lines = resp.lines
          }

        case INVALID_MAILBOX_NAME =>
          new InvalidMailboxName(resp.info) {
            override val isMultiline = resp.isMultiline
            override val lines = resp.lines
          }

        case TRANSACTION_FAILED =>
          new TransactionFailed(resp.info) {
            override val isMultiline = resp.isMultiline
            override val lines = resp.lines
          }

        case ADDRESS_NOT_RECOGNIZED =>
          new AddressNotRecognized(resp.info) {
            override val isMultiline = resp.isMultiline
            override val lines = resp.lines
          }

        case _ =>
          new UnknownReplyCodeError(resp.code, resp.info) {
            override val isMultiline = resp.isMultiline
            override val lines = resp.lines
          }
      }
    }
  }

  /**
   * Satisfies given promise with the specified version of a given reply
   * and logs it.
   *
   * @param rep The reply to specify
   * @param p   The satisfied promise
   */
  private def decodeReply(rep: UnspecifiedReply, p: Promise[Reply]) = {
    if (rep.isMultiline) {
      val start = "server:\r\n" + rep.code + "-"
      val middle = rep.lines.dropRight(1).mkString("\r\n" + rep.code + "-")
      val end = "\r\n" + rep.code + " " + rep.lines.last

      log.trace("%s%s%s", start, middle, end)
    }
    else log.trace("server: %d %s", rep.code, rep.info)

    getSpecifiedReply(rep) match {
      case err: Error => p.setException(err)
      case r@_ => p.setValue(r)
    }
  }

}
