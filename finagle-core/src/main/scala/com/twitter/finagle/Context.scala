package com.twitter.finagle

import com.twitter.finagle.util.LoadService
import com.twitter.finagle.tracing.TraceContext
import com.twitter.io.Buf
import com.twitter.logging.Level
import com.twitter.util.{Local, NonFatal}
import java.nio.charset.Charset

/**
 * A context is a piece of serializable metadata managed by a
 * registered handler. Protocol implementations may enumerate current
 * contexts to associate with outgoing messages; incoming contexts
 * are handled.
 *
 * Each context type is associated with a key. Keys with no
 * registered handlers are assigned a pass-thru handler: they are
 * uninterpreted, but are propagated across requests.
 *
 * Context handlers implement ContextHandler and are loaded via
 * the Java service loader mechanism.
 */
object Context {
  private val log = util.DefaultLogger

  private val loadedHandlers = LoadService[ContextHandler]()
  private val predefHandlers = Seq(new TraceContext)

  @volatile private var handlers: Array[ContextHandler] =
    (loadedHandlers ++ predefHandlers).toArray

  for (h <- handlers) {
    val Buf.Utf8(key) = h.key
    log.log(Level.DEBUG, "Context: added handler "+key)
  }

  private[finagle] def keyBytes(key: Buf): Array[Byte] = {
    val bytes = new Array[Byte](key.length)
    key.write(bytes, 0)
    bytes
  }

  private def getOrAddHandler(key: Buf): ContextHandler = synchronized {
    handlers.find(_.key == key) match {
      case Some(h) => h
      case None =>
        val h = new PassthruContext(key)
        handlers :+= h
        h
    }
  }

  private def handlerOf(key: Buf): ContextHandler =
    handlers.find(_.key == key) match {
      case Some(h) => h
      case None => getOrAddHandler(key)
    }

  private class PassthruContext(val key: Buf) extends ContextHandler {
    private[this] val local = new Local[Buf]
    def handle(buf: Buf) { local() = buf }
    def emit() = local()
  }

  /**
   * Handle the buffer as a serialized context with the
   * given key.
   */
  def handle(key: Buf, buf: Buf) {
    try handlerOf(key).handle(buf) catch {
      case NonFatal(exc) =>
        log.log(Level.WARNING, "Exception while handling request context", exc)
    }
  }

  /**
   * Emit all active contexts in the form of key-value tuples.
   */
  def emit(): Iterable[(Buf, Buf)] = for {
    h <- handlers
    v <- h.emit()
  } yield (h.key, v)
}

/**
 * A ContextHandler is responsible for maintaining a context.
 * Protocol implementations use it for handling incoming contexts and
 * to serialize outgoing ones. The contexts are themselves opaque to
 * protocol implementations.
 */
trait ContextHandler {
  /**
   * The key of the context handled by this context handler. It is
   * typically a Utf-8 encoded, fully-qualified class name.
   */
  val key: Buf

  /**
   * Handle is called by protocol implementations to hand off
   * a request context.
   */
  def handle(body: Buf)

  /**
   * Emit the current, serialized value of this context.
   */
  def emit(): Option[Buf]
}
