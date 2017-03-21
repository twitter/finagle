package com.twitter.finagle.mux.transport

import com.twitter.concurrent.{AsyncQueue, Broker, Offer}
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.{Failure, Status}
import com.twitter.io.{Buf, ByteReader, ByteWriter}
import com.twitter.util.{Future, Promise,Time, Throw, Return}
import java.net.SocketAddress
import java.security.cert.Certificate
import java.util.concurrent.atomic.AtomicInteger
import scala.util.control.NonFatal

/**
 * Defines a [[com.twitter.finagle.transport.Transport]] which allows a
 * mux session to be shared between multiple tag streams. The transport splits
 * mux messages into fragments with a size defined by a parameter. Writes are
 * then interleaved to achieve equity and goodput over the entire stream.
 * Fragments are aggregated into complete mux messages when read. The fragment size
 * is negotiated when a mux session is initialized.
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
    val KeyBuf: Buf = Buf.Utf8("mux-framer")

    /**
     * Returns a header value with the given frame `size` encoded.
     */
    def encodeFrameSize(size: Int): Buf = {
      require(size > 0)
      val bw = ByteWriter.fixed(4)
      bw.writeIntBE(size)
      bw.owned()
    }

    /**
     * Extracts frame size from the `buf`.
     */
    def decodeFrameSize(buf: Buf): Int = {
      val size = ByteReader(buf).readIntBE()
      require(size > 0)
      size
    }
  }

  /**
   * Represents a tag stream while writing fragments. To avoid unncessary allocations
   * `FragmentStream` carries some mutable state. In particular, `fragments` is a mutable
   * iterator and its contents should not be written concurrently.
   */
  case class FragmentStream(
    tag: Int,
    fragments: Iterator[Buf],
    writePromise: Promise[Unit])

  /**
   * Represents an interrupt for a stream.
   */
  case class Interrupt(tag: Int, exc: Throwable)

  /**
   * Creates a new [[Transport]] which fragments writes into `writeWindowBytes`
   * sized payloads and defragments reads into mux [[Message]]s.
   *
   * @param writeWindowBytes messages larger than this value are fragmented on
   * write. If the value is not defined, writes are proxied to the underlying
   * transport. However, the transport is always prepared to read fragments.
   */
  def apply(
    trans: Transport[Buf, Buf],
    writeWindowBytes: Option[Int],
    statsReceiver: StatsReceiver
  ): Transport[Message, Message] = new Transport[Message, Message] {
    require(writeWindowBytes.isEmpty || writeWindowBytes.exists(_ > 0),
      s"writeWindowBytes must be positive: $writeWindowBytes")

    // stats for both read and write paths
    private[this] val pendingWriteStreams, pendingReadStreams = new AtomicInteger(0)
    private[this] val writeStreamBytes = statsReceiver.stat("write_stream_bytes")
    private[this] val readStreamBytes = statsReceiver.stat("read_stream_bytes")
    private[this] val gauges = Seq(
      statsReceiver.addGauge("pending_write_streams") { pendingWriteStreams.get },
      statsReceiver.addGauge("pending_read_streams") { pendingReadStreams.get },
      statsReceiver.addGauge("write_window_bytes") {
        writeWindowBytes match {
          case Some(bytes) => bytes.toFloat
          case None => -1F
        }
      }
    )

    /**
     * Returns an iterator over the fragments of `msg`. Each fragment is sized to
     * be <= `maxSize`. Note, this should not be iterated over on more than one
     * thread at a time.
     */
    private[this] def fragment(msg: Message, maxSize: Int): Iterator[Buf] =
      if (msg.buf.length <= maxSize) {
        Iterator.single(Message.encode(msg))
      } else new Iterator[Buf] {
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

        private[this] val headerBuf = Buf.ByteArray.Owned(header)
        private[this] val buf = msg.buf
        private[this] val readable = buf.length

        @volatile private[this] var read = 0

        def hasNext: Boolean = read < readable

        def next(): Buf = {
          if (!hasNext) throw new NoSuchElementException

          if (readable - read <= maxSize) {
            // Toggle the tag MSB in the header region which signifies
            // the end of the sequence. Note, our header is denormalized
            // across 4 bytes.
            header(1) = (header(1) ^ (1 << 7)).toByte
          }
          // ensure we don't slice past the end of the msg.buf
          val frameLength = math.min(readable - read, maxSize)
          // note that the size of a frame is implicitly prepended by the transports
          // pipeline and is derived from readable bytes.
          val b = headerBuf.concat(buf.slice(from = read, until = read + frameLength))
          read += frameLength
          b
        }
      }

    // Queues incoming streams which are dequeued and
    // flushed in `writeLoop`.
    private[this] val writeq = new Broker[FragmentStream]

    // Kicks off a new writeLoop with an incoming stream.
    private[this] val newWriteLoop: Offer[Unit] = writeq.recv.map { stream =>
      writeLoop(Seq(stream))
    }

    // Communicates interrupts for outstanding streams.
    private[this] val interrupts = new Broker[Interrupt]

    // This is lifted out of writeLoop to avoid a closure. Technically, we could
    // inline `Offer.const(writeLoop)` in the loop, but this makes it difficult to
    // feign concurrency over a single thread because of how the LocalScheduler is
    // implemented.
    private[this] val unitOffer = Offer.const(())

    /**
     * Write fragments from `streams` recursively. Each iteration, a layer
     * from `streams` is written to the transport, effectively load balancing
     * across all streams to ensure a diverse and equitable session. New streams
     * join the `writeLoop` via the `writeq` broker and are interrupted via the
     * `interrupts` broker.
     *
     * @note The order in which we iterate over `streams` (and thus write to
     * the transport) isn't strictly guaranteed and can change in the presence
     * of interrupts, for example.
     */
    private[this] def writeLoop(streams: Seq[FragmentStream]): Future[Unit] =
      if (streams.isEmpty) newWriteLoop.sync() else {
        val round = streams.foldLeft[Seq[Future[FragmentStream]]](Nil) {
          case (writes, s@FragmentStream(_, fragments, writep)) if fragments.hasNext =>
            val buf = fragments.next()
            writeStreamBytes.add(buf.length)
            val write = trans.write(buf).transform {
              case Return(_) => Future.value(s)
              case exc@Throw(_) =>
                // `streams` should only contain streams where the
                // the write promise is not complete because interrupted
                // streams are filtered out before entering `writeLoop`.
                writep.update(exc)
                Future.value(s)
            }
            write +: writes
          case (writes, FragmentStream(_, _, writep)) =>
            // We have completed the stream. It's not possible for
            // `writep` to be complete since interrupted streams are
            // guaranteed to be filtered out of `streams`.
            writep.update(Return.Unit)
            writes
        }
        // Note, we don't need to `collectToTry` here because `round` always
        // completes succesfully. Failures to write per-stream are encoded in
        // the stream's `writePromise`.
        Future.collect(round).flatMap { nextStreams =>
          // After each round, we choose between the following cases
          // (note, if more than one offer is available we choose the
          // first available w.r.t to the argument order):
          Offer.prioritize[Unit](
            // 1. Remove a stream which has been interrupted. We interrupt first
            // to allow a backup in `writeq` to be drained on interrupts. Note that
            // an interrupt before an element reaches the writeq is possible and
            // handled in `write`.
            interrupts.recv.map { case Interrupt(tag, exc) =>
              writeLoop(nextStreams.foldLeft[Seq[FragmentStream]](Nil) {
                case (ss, FragmentStream(`tag`, _, writep)) =>
                  writep.update(Throw(exc))
                  ss
                case (ss, s) => s +: ss
              })
            },
            // 2. Add an incoming stream.
            writeq.recv.map { s => writeLoop(s +: nextStreams) },
            // 3. Dispatch another round of writes.
            unitOffer.map { _ => writeLoop(nextStreams) }
          ).sync()
        }
      }

    // kick off the loop.
    newWriteLoop.sync()

    def write(msg: Message): Future[Unit] =
      if (writeWindowBytes.isEmpty) {
        trans.write(Message.encode(msg))
      } else msg match {
        // The sender of a Tdispatch has indicated it is no longer
        // interested in the request, in which case, we need to make
        // sure the Tdispatch is removed from the writeLoop if it
        // exists.
        case m@Message.Tdiscarded(tag, why) =>
          val intr = interrupts ! Interrupt(tag, Failure(why))
          intr.before { trans.write(Message.encode(m)) }

        case m: Message =>
          val p = new Promise[Unit]
          p.setInterruptHandler { case NonFatal(exc) =>
            // if an Rdispatch stream is interrupted, we send the
            // receiver an `Rdiscarded` so they can safely relinquish
            // any outstanding fragments and we remove the pending stream
            // from our `writeLoop`. Note, `Tdiscarded` is handled above.
            if (m.typ == Message.Types.Rdispatch) {
              val intr = interrupts ! Interrupt(m.tag, exc)
              // We make sure to interrupt before sending the Rdiscarded
              // so we can sequence the discard relative to fragments sitting
              // in the writeLoop.
              intr.before { trans.write(Message.encode(Message.Rdiscarded(m.tag))) }
            }
          }
          pendingWriteStreams.incrementAndGet()
          // There is no upper bound on writeq and elements can only
          // be removed via interrupts. However, the transport can
          // be bounded which is the underlying resource backing the
          // writeq.
          val nq = writeq ! FragmentStream(m.tag, fragment(m, writeWindowBytes.get), p)
          nq.before(p).ensure {
            pendingWriteStreams.decrementAndGet()
          }
      }

    /**
     * Stores fully aggregated mux messages that were read from `trans`.
     */
    private[this] val readq = new AsyncQueue[Message]

    /**
     * The `readLoop` is responsible for demuxing and defragmenting tag streams.
     * If we read an `Rdiscarded` remove the corresponding stream from `tags`.
     */
    private[this] def readLoop(tags: Map[Int, Buf]): Future[Unit] =
      trans.read().flatMap { buf =>
        readStreamBytes.add(buf.length)
        val br = ByteReader(buf)
        val header = br.readIntBE()
        val typ = Message.Tags.extractType(header)
        val tag = Message.Tags.extractTag(header)

        val isFragment = Message.Tags.isFragment(tag)

        // normalize tag by flipping the tag MSB
        val t = Message.Tags.setMsb(tag)

        // Both a transmitter or receiver can discard a stream.
        val discard = typ == Message.Types.BAD_Tdiscarded ||
          typ == Message.Types.Rdiscarded

        // We only want to intercept discards in this loop if we
        // are processing the stream.
        val nextTags = if (discard && tags.contains(tag)) {
          tags - tag
        } else if (isFragment) {
          // Append the fragment to the respective `tag` in `tags`.
          // Note, we don't reset the reader index because we want
          // to consume the header for fragments.
          tags.updated(t, tags.get(t) match {
            case Some(buf0) => buf0.concat(br.readAll())
            case None => br.readAll()
          })
        } else {
          // If the fragment bit isn't flipped, the `buf` is either
          // a fully buffered message or the last fragment for `tag`.
          // We distinguish between the two by checking for the presence
          // of `tag` in `tags`.
          val resBuf = if (!tags.contains(t)) buf else {
            val head = buf.slice(0, 4)
            val rest = tags(t)
            val last = buf.slice(4, buf.length)
            Buf(Seq(head, rest, last))
          }
          readq.offer(Message.decode(resBuf))
          tags - t
        }
        pendingReadStreams.set(nextTags.size)
        readLoop(nextTags)
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
    def close(deadline: Time): Future[Unit] = trans.close(deadline)
  }
}
