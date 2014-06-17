package com.twitter.finagle.smtp

import com.twitter.finagle.transport.Transport
import com.twitter.finagle.dispatch.GenSerialClientDispatcher
import com.twitter.util.{Future, Promise, Try}
import com.twitter.finagle.smtp.reply._

object SmtpClientDispatcher {
  private def makeUnit[T](p: Promise[T], value: => T): Future[Unit] = {
   p.updateIfEmpty(Try(value))
   Future.Done
  }
}

class SmtpClientDispatcher(trans: Transport[Request, UnspecifiedReply])
extends GenSerialClientDispatcher[Request, Reply, Request, UnspecifiedReply](trans){
  import GenSerialClientDispatcher.wrapWriteException
  import SmtpClientDispatcher._
  import ReplyCode._

  /*Connection phase: should receive greeting from the server*/
  private val connPhase: Future[Unit] = {
    trans.read flatMap { greet =>
      val p = new Promise[Reply]
      decodeReply(greet, p) flatMap { unit =>
        p flatMap {
          case ServiceReady(info) => Future.Done
          case other => Future.exception(InvalidReply(other.toString))
        }
      }
    } onFailure {
      case _ =>  close()
    }
  }

  /**
   * Dispatch a request, satisfying Promise `p` with the response;
   * the returned Future is satisfied when the dispatch is complete:
   * only one request is admitted at any given time.
   */
  protected def dispatch(req: Request, p: Promise[Reply]): Future[Unit] = {
    connPhase flatMap { _ =>
      trans.write(req) rescue {
        wrapWriteException
      } flatMap { unit =>
        trans.read()
      } flatMap { rp =>
        decodeReply(rp, p)
      }
    } onFailure {
      _ => close()
    }
  }

  private def getSpecifiedReply(resp: UnspecifiedReply): Reply = resp match {
    case specified: Reply => specified
    case _: UnspecifiedReply => {
      resp.code match {
        case SYSTEM_STATUS              => new SystemStatus(resp.info) {
                                                override val isMultiline = resp.isMultiline
                                                override val lines = resp.lines
                                                }
        case HELP                       => new Help(resp.info)  {
                                                override val isMultiline = resp.isMultiline
                                                override val lines = resp.lines
                                            }
        case SERVICE_READY              => new ServiceReady(resp.info)  {
                                                 override val isMultiline = resp.isMultiline
                                                 override val lines = resp.lines
                                               }
        case CLOSING_TRANSMISSION       => new ClosingTransmission(resp.info)  {
                                                 override val isMultiline = resp.isMultiline
                                                 override val lines = resp.lines
                                               }
        case OK                         => new OKReply(resp.info)  {
                                                 override val isMultiline = resp.isMultiline
                                                 override val lines = resp.lines
                                               }
        case TEMP_USER_NOT_LOCAL        => new TempUserNotLocal(resp.info)  {
                                                 override val isMultiline = resp.isMultiline
                                                 override val lines = resp.lines
                                               }
        case TEMP_USER_NOT_VERIFIED     => new TempUserNotVerified(resp.info)  {
                                                 override val isMultiline = resp.isMultiline
                                                 override val lines = resp.lines
                                               }
        case START_INPUT                => new StartInput(resp.info)  {
                                                 override val isMultiline = resp.isMultiline
                                                 override val lines = resp.lines
                                               }
        case SERVICE_NOT_AVAILABLE      => new ServiceNotAvailable(resp.info)  {
                                                 override val isMultiline = resp.isMultiline
                                                 override val lines = resp.lines
                                               }
        case TEMP_MAILBOX_UNAVAILABLE   => new TempMailboxUnavailable(resp.info)  {
                                                 override val isMultiline = resp.isMultiline
                                                 override val lines = resp.lines
                                               }
        case PROCESSING_ERROR           => new ProcessingError(resp.info)  {
                                                 override val isMultiline = resp.isMultiline
                                                 override val lines = resp.lines
                                               }
        case TEMP_INSUFFICIENT_STORAGE  => new TempInsufficientStorage(resp.info)  {
                                                 override val isMultiline = resp.isMultiline
                                                 override val lines = resp.lines
                                               }
        case PARAMS_ACCOMODATION_ERROR  => new ParamsAccommodationError(resp.info)  {
                                                 override val isMultiline = resp.isMultiline
                                                 override val lines = resp.lines
                                               }
        case SYNTAX_ERROR               => new SyntaxError(resp.info)  {
                                                 override val isMultiline = resp.isMultiline
                                                 override val lines = resp.lines
                                               }
        case ARGUMENT_SYNTAX_ERROR      => new ArgumentSyntaxError(resp.info)  {
                                                 override val isMultiline = resp.isMultiline
                                                 override val lines = resp.lines
                                               }
        case COMMAND_NOT_IMPLEMENTED    => new CommandNotImplemented(resp.info)  {
                                                 override val isMultiline = resp.isMultiline
                                                 override val lines = resp.lines
                                               }
        case BAD_COMMAND_SEQUENCE       => new BadCommandSequence(resp.info)  {
                                                 override val isMultiline = resp.isMultiline
                                                 override val lines = resp.lines
                                               }
        case PARAMETER_NOT_IMPLEMENTED  => new ParameterNotImplemented(resp.info)  {
                                                 override val isMultiline = resp.isMultiline
                                                 override val lines = resp.lines
                                               }
        case MAILBOX_UNAVAILABLE_ERROR  => new MailboxUnavailableError(resp.info)  {
                                                 override val isMultiline = resp.isMultiline
                                                 override val lines = resp.lines
                                               }
        case USER_NOT_LOCAL_ERROR       => new UserNotLocalError(resp.info)  {
                                                 override val isMultiline = resp.isMultiline
                                                 override val lines = resp.lines
                                               }
        case INSUFFICIENT_STORAGE_ERROR => new InsufficientStorageError(resp.info)  {
                                                 override val isMultiline = resp.isMultiline
                                                 override val lines = resp.lines
                                               }
        case INVALID_MAILBOX_NAME       => new InvalidMailboxName(resp.info)  {
                                                 override val isMultiline = resp.isMultiline
                                                 override val lines = resp.lines
                                               }
        case TRANSACTION_FAILED         => new TransactionFailed(resp.info)  {
                                                 override val isMultiline = resp.isMultiline
                                                 override val lines = resp.lines
                                               }
        case ADDRESS_NOT_RECOGNIZED     => new AddressNotRecognized(resp.info)  {
                                                 override val isMultiline = resp.isMultiline
                                                 override val lines = resp.lines
                                               }

        case _                          => new UnknownReplyCodeError(resp.code, resp.info)  {
                                                 override val isMultiline = resp.isMultiline
                                                 override val lines = resp.lines
                                               }
      }
    }
  }

  private def decodeReply(rep: UnspecifiedReply, p: Promise[Reply]): Future[Unit] = makeUnit(p, getSpecifiedReply(rep))

}
