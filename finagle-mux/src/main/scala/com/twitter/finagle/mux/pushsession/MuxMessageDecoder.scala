package com.twitter.finagle.mux.pushsession

import com.twitter.finagle.mux.transport.Message
import com.twitter.finagle.mux.transport.Message.{Tags, Tdiscarded}
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.io.{Buf, ByteReader}
import io.netty.util.collection.IntObjectHashMap

private[finagle] abstract class MuxMessageDecoder {

  /**
   * Decode a `ByteReader` into a `Message`
   *
   * The decoder takes ownership of the passed `ByteReader` in that the
   * `ByteReader.close()` method is called before returning.
   *
   * @note this can return `null` in the case that a fragment was received.
   */
  final def decode(reader: ByteReader): Message = {
    try doDecode(reader)
    finally reader.close()
  }

  /**
   * Decode a `ByteReader` into a `Message`. This method _must not_ take ownership
   * of the provided `ByteReader`. Eg, it _must not_ call its `close()` method.
   */
  protected def doDecode(reader: ByteReader): Message
}

private[mux] final class FragmentDecoder(stats: SharedNegotiationStats) extends MuxMessageDecoder {

  // While this constructor defeats the purprose of sharing stats between multiple readers,
  // it's convenient in testing.
  def this(stats: StatsReceiver) =
    this(new SharedNegotiationStats(stats))

  // The keys of the fragment map are 'normalized' since fragments are signaled
  // in the MSB of the tag field. See `getKey` below.
  private[this] val fragments = new IntObjectHashMap[Buf]

  // Doesn't take ownership of the `ByteReader`
  protected def doDecode(reader: ByteReader): Message = {
    stats.readStreamBytes.add(reader.remaining)

    val header = reader.readIntBE()
    val typ = Tags.extractType(header)
    val tag = Tags.extractTag(header)

    if (!Message.Tags.isFragment(tag)) lastChunk(tag, typ, reader)
    else {
      accumulateFragment(tag, reader)
      null
    }
  }

  // Takes the last fragment of a message, which is potentially the only
  // fragment or a fully buffered message, and appends it to any existing
  // data then decodes to a mux Message.
  private[this] def lastChunk(tag: Int, typ: Byte, reader: ByteReader): Message = {
    if (Message.Types.isDiscard(typ)) {
      val msg = Message.decodeMessageBody(typ, tag, reader)
      // We have to special case the Tdiscarded since it is a marker-message
      // and the dispatch tag is encoded in the message's `which` field.
      val tagToRemove = msg match {
        case Tdiscarded(tagToRemove, _) => tagToRemove
        case _ => tag
      }

      if (fragments.remove(getKey(tagToRemove)) != null)
        stats.pendingReadStreams.decrement()

      msg
    } else {
      val existing = fragments.remove(getKey(tag))
      val fullMessageBody =
        if (existing == null) reader
        else ByteReader(existing.concat(reader.readAll()))

      if (existing != null)
        stats.pendingReadStreams.decrement()

      Message.decodeMessageBody(typ, tag, fullMessageBody)
    }
  }

  private[this] def accumulateFragment(tag: Int, reader: ByteReader): Unit = {
    val key = getKey(tag)
    val tail = reader.readAll()
    val head = fragments.get(key)
    val chunk = if (head != null) head.concat(tail) else tail
    fragments.put(key, chunk)

    if (head == null)
      stats.pendingReadStreams.increment()
  }

  // All fragments are stored with their tags 'normalized' since the
  // fragments for a tag x have tag x | (1 << 23) and thus the tag
  // read from the wire is different.
  private[this] def getKey(tag: Int): Int = Message.Tags.setMsb(tag)
}
