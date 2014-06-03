package com.twitter.finagle.smtp

import com.twitter.finagle.transport.Transport
import com.twitter.finagle.dispatch.GenSerialClientDispatcher
import com.twitter.util.{Future, Promise, Try}

class SmtpClientDispatcher(trans: Transport[String, Reply])
extends GenSerialClientDispatcher[Request, Reply, String, Reply](trans){
import GenSerialClientDispatcher.wrapWriteException

  /**
   * Dispatch a request, satisfying Promise `p` with the response;
   * the returned Future is satisfied when the dispatch is complete:
   * only one request is admitted at any given time.
   */
  protected def dispatch(req: Request, p: Promise[Reply]): Future[Unit] = req match {
    case SingleRequest(cmd) => trans.write(cmd) rescue {
      wrapWriteException
    } flatMap { unit =>
      trans.read()
      } flatMap {rsp =>
         decodeResponse(rsp, p)
      }
  }

  private def decodeResponse(resp: Reply, p: Promise[Reply]): Future[Unit] = {
    if (!resp.isValid) Future(p.updateIfEmpty(Try(resp)))
    else {
      val concreteReply = Future {
        resp.getCode match {
          case 211 => SystemStatus(resp.info)
          case 214 => Help(resp.info)
          case 220 => ServiceReady(resp.info)//TODO: multiline
          case 221 => ClosingTransmission(resp.info)
          case 250 => OK(resp.info)
          case 251 => TempUserNotLocal(resp.info)
          case 252 => TempUserNotVerified(resp.info)

          case 354 => StartInput(resp.info)

          case 421 => ServiceNotAvailable(resp.info)
          case 450 => TempMailboxUnavailable(resp.info)
          case 451 => ProcessingError(resp.info)
          case 452 => TempInsufficientStorage(resp.info)
          case 455 => ParamsAccommodationError(resp.info)

          case 500 => SyntaxError(resp.info)
          case 501 => ArgumentSyntaxError(resp.info)
          case 502 => CommandNotImplemented(resp.info)
          case 503 => BadCommandSequence(resp.info)
          case 504 => ParameterNotImplemented(resp.info)
          case 550 => MailboxUnavailableError(resp.info)
          case 551 => UserNotLocalError(resp.info)
          case 552 => InsufficientStorageError(resp.info)
          case 553 => InvalidMailboxName(resp.info)
          case 554 => TransactionFailed(resp.info)
          case 555 => AddressNotRecognized(resp.info)

          case _ => UnknownReplyCodeError(resp.firstDigit, resp.secondDigit, resp.thirdDigit, resp.info)
        }
      }

    Future(p.become(concreteReply))

    }
  }

}
