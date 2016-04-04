package com.twitter.finagle.mux.transport

import com.twitter.concurrent.{AsyncQueue, NamedPoolThreadFactory}
import com.twitter.conversions.time._
import com.twitter.finagle.Failure
import com.twitter.finagle.Status
import com.twitter.finagle.transport.Transport
import com.twitter.io.Charsets
import com.twitter.util._
import java.net.SocketAddress
import java.security.cert.Certificate
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.ConcurrentLinkedQueue
import org.jboss.netty.buffer.ChannelBuffers.{unmodifiableBuffer, wrappedBuffer}
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}

/**
 * Defines a [[com.twitter.finagle.transport.Transport]] which allows a
 * mux session to be shared between multiple tag streams. The transport operates
 * over a mux frame and splits mux data messages into fragments. Fragments
 * are aggregated into complete mux messages when read. The size of fragments is
 * defined by a parameterized `window` and negotiated when a mux session is initialized.
 *
 * @see [[com.twitter.finagle.mux.Handshake]] for usage details.
 *
 * @note Our current implementation does not offer any mechanism to resize
 * the window after a session is established. However, it is possible to
 * compose a flow control algorithm over this which can dynamically control
 * the size of `window`.
 */
private[finagle] object MuxFramer {
  /**
   * Defines mux framer keys and values exchanged as part of a
   * mux session header during initialization.
   */
  object Header {
    private val key = "mux-framer".getBytes(Charsets.Utf8)
    def KeyBuf: ChannelBuffer = unmodifiableBuffer(wrappedBuffer(key))

    /**
     * Returns a header value with the given frame `size` encoded.
     */
    def encodeFrameSize(size: Int): ChannelBuffer = {
      require(size > 0)
      val cb = ChannelBuffers.buffer(4)
      cb.writeInt(size)
      unmodifiableBuffer(cb)
    }

    /**
     * Extracts frame size from the `cb`.
     */
    def decodeFrameSize(cb: ChannelBuffer): Int = {
      val size = cb.readInt()
      require(size > 0)
      size
    }
  }

  /**
   * Represents a tag stream while writing fragments.
   */
  case class Stream(
    typ: Byte,
    tag: Int,
    fragments: Iterator[ChannelBuffer],
    writePromise: Promise[Unit])

  /**
   * Timer used to ensure that we are making progress with flushes. Note, this is
   * shared across all active mux sessions.
   *
   * @note we use a ScheduledThreadPoolTimer because it offers more
   * granular scheduling than the alternative timers.
   */
  val flushTimer: Timer = new ScheduledThreadPoolTimer(
    poolSize = 1, new NamedPoolThreadFactory("flow-writer", makeDaemons = true), None)

  /**
   * Creates a new [[com.twitter.finagle.transport.Transport]] which
   * fragments writes into `windowBytes` sized payloads and defrags reads
   * into mux [[Message]]s.
   */
  def apply(
    trans: Transport[ChannelBuffer, ChannelBuffer],
    windowBytes: Int
  ): Transport[Message, Message] = new Transport[Message, Message] {
    require(windowBytes > 0, s"windowBytes must be positive: $windowBytes")

    /**
     * Returns an iterator over the fragments of `msg`. Each fragment is sized to
     * be <= `windowBytes`. Note, this should not be iterated over on more than one
     * thread at a time.
     */
    private[this] def fragment(msg: Message): Iterator[ChannelBuffer] =
      new Iterator[ChannelBuffer] {
        // Create a mux header with the tag MSB set to 1. This signifies
        // that the message is part of a set of fragments. Note that the
        // decoder needs to respect this and not attempt to decode fragments
        // past their headers.
        private[this] val header: Array[Byte] = {
          val tag = Message.Tags.setMsb(msg.tag)
          Array[Byte](msg.typ,
            (tag >> 16 & 0xff).toByte,
            (tag >> 8 & 0xff).toByte,
            (tag & 0xff).toByte
          )
        }

        private[this] val headerBuf = unmodifiableBuffer(wrappedBuffer(header))
        headerBuf.markReaderIndex()

        private[this] val readable = msg.buf.readableBytes
        @volatile private[this] var read = 0

        def hasNext: Boolean = read < readable

        def next(): ChannelBuffer = {
          if (!hasNext) throw new NoSuchElementException

          headerBuf.resetReaderIndex()
          if (readable - read <= windowBytes) {
            // Toggle the tag MSB in the header region which signifies
            // the end of the sequence. Note, our header is denormalized
            // across 4 bytes.
            header(1) = (header(1) ^ (1 << 7)).toByte
          }
          // ensure we don't slice past the end of the msg.buf
          val len = math.min(readable - read, windowBytes)
          // note that the size of a frame is implicitly prepended by the transports
          // pipeline and is derived from `readableBytes.`
          val b = wrappedBuffer(headerBuf, msg.buf.slice(read, len))
          read += len
          b
        }
      }

    // Unfortunately, in netty3 all writes are eagerly flushed to the socket buffer.
    // What's more, writing large buffers can disproportionally occupy the I/O thread
    // event loop with writes and starve reads. In order to properly interleave streams
    // and achieve goodput, we queue and defer writes for large streams. The queue is
    // flushed on every read and on a timer to ensure that low throughput servers don't
    // add too much latency to writes.
    //
    // Note, netty4 allows for a distinction between writes and flushes and would allow
    // us to schedule flushes on the event loop thread instead.
    private[this] val writeq = new ConcurrentLinkedQueue[Stream]

    private[this] val flushing = new AtomicBoolean(false)
    private[this] val flushTask = flushTimer.schedule(1.millis) { flushWrites() }

    /**
     * Flush fragments from `writeq` to the underlying transport. Fragments are written
     * sequentially per stream, in accordance with the mux spec, and round robin across
     * all active streams. If a tag stream is interrupted, the stream isn't requeued and
     * we send the receiver an `Rdiscarded` so they can safely relinquish any outstanding
     * fragments.
     *
     * Note, ideally we would write each round followed by a flush, but there is no
     * way for us to do this sort of batching at this level. In netty4, we can push
     * flushing down into the ChannelTransport and get batching for free.
     */
    private[this] def flushWrites(): Unit =
      if (flushing.compareAndSet(false, true)) {
        val iter = writeq.iterator
        while (iter.hasNext) {
          val stream = iter.next()
          iter.remove()
          val Stream(typ, tag, fragments, writep) = stream
          // Note, we interrupt `writep` lazily (i.e. no interrupt handler)
          // because this allows us to do it in-band with flushes,
          // rather than requiring coordination.
          writep.isInterrupted match {
            case Some(intr) =>
              // Note, Tdiscarded are sent by the client
              // and handled in `write`.
              if (typ < 0) write(Message.Rdiscarded(tag))
              writep.updateIfEmpty(Throw(intr))
            case None =>
              if (!fragments.hasNext) writep.setDone() else {
                trans.write(fragments.next()).respond {
                  case Return(_) => writeq.offer(stream)
                  case exc@Throw(_) => writep.update(exc)
                }
              }
          }
        }
        flushing.set(false)
      }

    def write(msg: Message): Future[Unit] = msg match {
      case m@(_: Message.Tdispatch | _: Message.Rdispatch)
        if m.buf.readableBytes > windowBytes =>
          val fragments = fragment(m)
          // write the head, enqueue the tail. We are guaranteed
          // for fragments.hasNext to be true since our readable
          // bytes is greater than window.
          trans.write(fragments.next()).before {
            val p = new Promise[Unit]
            writeq.offer(Stream(m.typ, m.tag, fragments, p))
            p
          }

      case m@Message.Tdiscarded(tag, why) =>
        val iter = writeq.iterator
        var removed = false
        while (iter.hasNext && !removed) {
          val s = iter.next()
          if (s.tag == tag) {
            iter.remove()
            s.writePromise.updateIfEmpty(Throw(Failure(why)))
            removed = true
          }
        }
        trans.write(Message.encode(m))

      case m => trans.write(Message.encode(m))
    }

    /**
     * Stores fully aggregated mux messages that were read from `trans`.
     */
    private[this] val readq = new AsyncQueue[Message]

    /**
     * The `readLoop` is responsible for demuxing and defragmenting tag streams.
     * If we read an `Rdiscarded` remove the corresponding stream from `tags`.
     */
    private[this] def readLoop(tags: Map[Int, ChannelBuffer]): Future[Unit] =
      trans.read().flatMap { buf =>
        // See comment over `writeq` as to why we flush writes in read.
        flushWrites()
        buf.markReaderIndex()
        val header = buf.readInt()
        val typ = Message.Tags.extractType(header)
        val tag = Message.Tags.extractTag(header)

        val isFragment = Message.Tags.isFragment(tag)

        // normalize tag by flipping the tag MSB
        val t = Message.Tags.setMsb(tag)

        // Both a transmitter or receiver can discard a stream.
        val discard = typ == Message.Types.BAD_Tdiscarded ||
          typ == Message.Types.Tdiscarded

        // We only want to intercept discards in this loop if we
        // are processing the stream.
        if (discard && tags.contains(tag)) {
          readLoop(tags - tag)
        } else if (isFragment) {
          // Append the fragment to the respective `tag` in `tags`.
          // Note, we don't reset the reader index because we want
          // to consume the header for fragments.
          val nextTags = tags.updated(t, tags.get(t) match {
            case Some(buf0) => wrappedBuffer(buf0, buf)
            case None => buf
          })
          readLoop(nextTags)
        } else {
          // If the fragment bit isn't flipped, the `buf` is either
          // a fully buffered message or the last fragment for `tag`.
          // We distinguish between the two by checking for the presence
          // of `tag` in `tags`.
          buf.resetReaderIndex()
          val resBuf = if (!tags.contains(t)) buf else {
            val headerBuf = buf.readSlice(4)
            val buf0 = tags(t)
            wrappedBuffer(headerBuf, buf0, buf)
          }
          readq.offer(Message.decode(resBuf))
          readLoop(tags - t)
        }
      }

    // failures are pushed to the readq which are propagated to
    // the layers above.
    readLoop(Map.empty).onFailure { exc => readq.fail(exc) }

    def read(): Future[Message] = readq.poll()

    def status: Status = trans.status
    val onClose: Future[Throwable] = trans.onClose
    def localAddress: SocketAddress = trans.localAddress
    def remoteAddress: SocketAddress = trans.remoteAddress
    def peerCertificate: Option[Certificate] = trans.peerCertificate
    def close(deadline: Time): Future[Unit] = {
      flushTask.cancel()
      trans.close(deadline)
    }
  }
}
