package com.twitter.finagle.smtp

import com.twitter.finagle.transport.Transport
import com.twitter.finagle.dispatch.GenSerialClientDispatcher
import com.twitter.util.{Future, Promise, Try}
import scala.annotation.tailrec

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

  override def apply(req: Request): Future[Reply]  = {
    connPhase flatMap { _ =>
      val p = new Promise[Reply]
      dispatch(req, p)
      p
    } onFailure {
      case _ => close()
    }
  }

  /**
   * Dispatch a request, satisfying Promise `p` with the response;
   * the returned Future is satisfied when the dispatch is complete:
   * only one request is admitted at any given time.
   */
  protected def dispatch(req: Request, p: Promise[Reply]): Future[Unit] = {
    trans.write(req) rescue {
      wrapWriteException
    } flatMap { unit =>
      trans.read()
    } flatMap { rp =>
      decodeReply(rp, p)
      }
  }

  private def getSpecifiedReply(resp: UnspecifiedReply): Reply = resp match {
    case specified: Reply => specified
    case _: UnspecifiedReply => {
      resp.code match {
        case SYSTEM_STATUS => SystemStatus(resp.info)
        case HELP => Help(resp.info)
        case SERVICE_READY => ServiceReady(resp.info)
        case CLOSING_TRANSMISSION => ClosingTransmission(resp.info)
        case OK => OKReply(resp.info)
        case TEMP_USER_NOT_LOCAL => TempUserNotLocal(resp.info)
        case TEMP_USER_NOT_VERIFIED => TempUserNotVerified(resp.info)
        case START_INPUT => StartInput(resp.info)
        case SERVICE_NOT_AVAILABLE => ServiceNotAvailable(resp.info)
        case TEMP_MAILBOX_UNAVAILABLE => TempMailboxUnavailable(resp.info)
        case PROCESSING_ERROR => ProcessingError(resp.info)
        case TEMP_INSUFFICIENT_STORAGE => TempInsufficientStorage(resp.info)
        case PARAMS_ACCOMODATION_ERROR => ParamsAccommodationError(resp.info)
        case SYNTAX_ERROR => SyntaxError(resp.info)
        case ARGUMENT_SYNTAX_ERROR => ArgumentSyntaxError(resp.info)
        case COMMAND_NOT_IMPLEMENTED => CommandNotImplemented(resp.info)
        case BAD_COMMAND_SEQUENCE => BadCommandSequence(resp.info)
        case PARAMETER_NOT_IMPLEMENTED => ParameterNotImplemented(resp.info)
        case MAILBOX_UNAVAILABLE_ERROR => MailboxUnavailableError(resp.info)
        case USER_NOT_LOCAL_ERROR => UserNotLocalError(resp.info)
        case INSUFFICIENT_STORAGE_ERROR => InsufficientStorageError(resp.info)
        case INVALID_MAILBOX_NAME => InvalidMailboxName(resp.info)
        case TRANSACTION_FAILED => TransactionFailed(resp.info)
        case ADDRESS_NOT_RECOGNIZED => AddressNotRecognized(resp.info)

        case _ => UnknownReplyCodeError(resp.code, resp.info)
      }
    }
  }

  private def decodeReply(rep: UnspecifiedReply, p: Promise[Reply]): Future[Unit] = rep match {
    case specified: Reply => makeUnit(p, specified) //covers Extension, OK and InvalidReply
    case unspecified: UnspecifiedReply =>  makeUnit(p, getSpecifiedReply(unspecified))
  }

}
