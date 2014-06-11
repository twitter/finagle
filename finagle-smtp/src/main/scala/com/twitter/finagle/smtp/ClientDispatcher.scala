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
        case 211 => SystemStatus(resp.info)
        case 214 => Help(resp.info)
        case 220 => ServiceReady(resp.info)
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

        case _ => UnknownReplyCodeError(resp.code, resp.info)
      }
    }
  }

  private def decodeReply(rep: UnspecifiedReply, p: Promise[Reply]): Future[Unit] = rep match {
    case Greeting(greet, rp) => makeUnit(p, Greeting(greet, getSpecifiedReply(rp)))
    case specified: Reply => makeUnit(p, specified) //covers Extension, OK and InvalidReply
    case unspecified: UnspecifiedReply =>  makeUnit(p, getSpecifiedReply(unspecified))
  }

}
