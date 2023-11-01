package com.twitter.finagle.http.codec.context

import com.twitter.finagle.context.Contexts
import com.twitter.finagle.http.HeaderMap
import com.twitter.finagle.http.Message
import com.twitter.finagle.util.LoadService
import com.twitter.logging.Level
import com.twitter.logging.Logger
import com.twitter.util.Return
import com.twitter.util.Throw
import com.twitter.util.Try
import scala.collection.mutable
import scala.util.control.NonFatal

/**
 * HttpContext is an interface to serialize finagle broadcast contexts
 * into http headers. All HttpContext headers are prefixed with `Finagle-Ctx-`.
 *
 * See [[LoadableHttpContext]] to include custom finagle broadcast contexts via
 * [[com.twitter.finagle.util.LoadService]].
 */
private[twitter] trait HttpContext {
  type ContextKeyType

  def key: Contexts.broadcast.Key[ContextKeyType]

  def id: String = key.id

  @volatile private[this] var _headerKey: String = null
  final def headerKey: String = {
    // There's a race condition here for setting _headerKey that
    // will occur on startup and worst-case causes a few extra string
    // allocations
    if (_headerKey == null)
      _headerKey = HttpContext.Prefix + id

    _headerKey
  }

  def toHeader(value: ContextKeyType): String
  def fromHeader(header: String): Try[ContextKeyType]

}

object HttpContext {

  private val log = Logger(getClass.getName)

  private[codec] val Prefix = "Finagle-Ctx-"

  private[this] val knownContextTypes: Array[HttpContext] = {
    Array[HttpContext](
      HttpDeadline,
      HttpRequeues,
      HttpBackupRequest
    )
  }

  private[this] val loadedContextTypes: Array[HttpContext] = {
    val loaded: mutable.ArrayBuffer[HttpContext] = scala.collection.mutable.ArrayBuffer.empty
    val ctxKeys: mutable.Set[String] = mutable.Set.empty ++ knownContextTypes.map(_.headerKey).toSet

    LoadService[LoadableHttpContext]().foreach { httpCtx =>
      if (ctxKeys.contains(httpCtx.headerKey)) {
        log.warning(s"skipping duplicate http header context key ${httpCtx.headerKey}")
      } else {
        ctxKeys += httpCtx.headerKey
        loaded += httpCtx
      }
    }

    loaded.toArray
  }

  // we differentiate between known and loaded context types for the `write` case, where the loaded
  // types cannot be added to the HeaderMap using `addUnsafe`. The key/value pairs for these must be validated
  private[this] val allContextTypes: Array[HttpContext] = knownContextTypes ++ loadedContextTypes

  /**
   * Remove the Deadline header from the Message. May be used
   * when it is not desirable for clients to be able to set
   * bogus or expired Deadline headers in an HTTP request.
   */
  def removeDeadline(msg: Message): Unit =
    msg.headerMap.remove(HttpDeadline.headerKey)

  /**
   * Read Finagle-Ctx header pairs from the given message for Contexts. This includes "Deadline",
   * "Retries", and "BackupRequests" Finagle contexts, as well as any contexts loaded via
   * [[LoadableHttpContext]]
   *
   * and run `fn`.
   */
  private[http] def read[R](msg: Message)(fn: => R): R = {
    var ctxValues: List[Contexts.broadcast.KeyValuePair[_]] = Nil

    var i: Int = 0
    while (i < allContextTypes.length) {
      val contextType = allContextTypes(i)

      // mutate `ctxValues` if the corresponding header is present and we can decode
      // the header successfully
      msg.headerMap.get(contextType.headerKey) match {
        case Some(header) =>
          contextType.fromHeader(header) match {
            case Return(ctxVal) =>
              ctxValues = Contexts.broadcast.KeyValuePair(contextType.key, ctxVal) :: ctxValues
            case Throw(_) =>
              if (log.isLoggable(Level.DEBUG))
                log.debug(s"could not unmarshal ${contextType.key} from the header value")
          }
        case None =>
      }

      i += 1
    }

    Contexts.broadcast.let(ctxValues)(fn)
  }

  /**
   * Write Finagle-Ctx header pairs to the given message for Contexts. This includes "Deadline",
   * "Retries", and "BackupRequests" Finagle contexts, as well as any contexts loaded via
   * [[LoadableHttpContext]]
   */
  private[http] def write(msg: Message): Unit = {
    var i = 0
    while (i < knownContextTypes.length) {
      val contextType = knownContextTypes(i)
      writeToHeader(contextType, msg.headerMap, isSafe = true)

      i += 1
    }

    i = 0
    while (i < loadedContextTypes.length) {
      val contextType = loadedContextTypes(i)
      writeToHeader(contextType, msg.headerMap, isSafe = false)

      i += 1
    }
  }

  private[this] def writeToHeader(
    contextType: HttpContext,
    headerMap: HeaderMap,
    isSafe: Boolean
  ): Unit = {
    Contexts.broadcast.get(contextType.key) match {
      case Some(ctxVal) =>
        if (isSafe) {
          headerMap.setUnsafe(contextType.headerKey, contextType.toHeader(ctxVal))
        } else {
          try {
            headerMap.set(contextType.headerKey, contextType.toHeader(ctxVal))
          } catch {
            case NonFatal(_) =>
              if (log.isLoggable(Level.DEBUG))
                log.debug(s"unable to add ${contextType.key} to the header")
          }
        }

      case None =>
    }
  }
}
