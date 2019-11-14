package com.twitter.finagle.mux.transport

import com.twitter.io.Buf
import com.twitter.logging.Logger
import java.nio.charset.StandardCharsets.UTF_8

private[finagle] object CompressionNegotiation {
  private[this] val log = Logger.get()

  /**
   * The compression formats negotiated for compressing requests and responses.
   *
   * The data structure used by the server to communicate to the client over the
   * wire how the request and response will be compressed.
   *
   * @param request the compression format that will be used for requests, or
   *                None if no compression format was negotiated.
   * @param response the compression format that will be used for responses, or
   *                 None if no compression format was negotiated.
   */
  private[mux] case class CompressionFormats(request: Option[String], response: Option[String]) {
    def isDisabled: Boolean = request == None && response == None
  }

  /**
   * Defines encrypter keys and values exchanged as part of a
   * mux session header during initialization.
   */
  private[finagle] object ClientHeader {

    /**
     * The key used for communicating to the server what the client is able to
     * compress and decompress.
     */
    val KeyBuf: Buf = Buf.Utf8("accept-compress")

    // format: "$compressionLevel:$compressors;$decompressionLevel:$compressors"
    /**
     * Encodes the value in the header that the client sends to the server,
     * communicating how the client would prefer to compress or not compress.
     *
     * The encoding is delimited by ';', ',', and ':', so the compression format
     * names can't have a ';', ',', or ':' in them.
     */
    def encode(preferences: Compression.LocalPreferences): Buf = {
      val compression = preferences.compression
      val decompression = preferences.decompression
      Buf.Utf8(s"${encodeSetting(compression)};${encodeSetting(decompression)}")
    }

    private[this] def encodeSetting(setting: Compression.LocalSetting): String = {
      val transformers = setting.transformers
      s"${setting.level.value}:${transformers.map(_.name).mkString(",")}"
    }

    /**
     * Decodes the header that the client sends to the server about its
     * compression preferences.
     */
    def decode(buf: Buf): Compression.PeerPreferences = {
      val string = Buf.decodeString(buf, UTF_8)
      string.split(';') match {
        case Array(compression, decompression) =>
          Compression.PeerPreferences(decodeSetting(compression), decodeSetting(decompression))
        case _ =>
          log.debug(s"Expected the Compression.PeerPreferences format but received: $string")
          Compression.PeerCompressionOff // a safe default is to not compress
      }
    }

    private[this] def decodeSetting(string: String): Compression.PeerSetting = {
      string.split(':') match {
        case Array(level, transformerNames) =>
          Compression.PeerSetting(decodeLevel(level), transformerNames.split(',').toSeq)
        case Array(level) => Compression.PeerSetting(decodeLevel(level), Nil)
        case _ =>
          log.debug(s"Expected the Compression.PeerSetting format but received: $string")
          Compression.PeerSetting(CompressionLevel.Off, Nil) // a safe default is to not compress
      }
    }

    private[this] def decodeLevel(string: String): CompressionLevel =
      if (string == CompressionLevel.Off.value) CompressionLevel.Off
      else if (string == CompressionLevel.Accepted.value) CompressionLevel.Accepted
      else if (string == CompressionLevel.Desired.value) CompressionLevel.Desired
      else {
        log.debug(s"Expected one of 'off', 'accepted', or 'desired' but received $string")
        CompressionLevel.Off // don't want to fail in case we decide to change levels in the future.
      }
  }

  private[finagle] object ServerHeader {

    /**
     * The key used for communicating to the client what compression formats
     * have been negotiated.
     */
    val KeyBuf: Buf = Buf.Utf8("compress")

    /**
     * Encodes the value in the header that the server returns to the client,
     * communicating what compression formats have been negotiated.
     *
     * The format is semicolon delimited, so the compression formats can't have
     * a ';' in them.
     */
    def encode(compressionFormats: CompressionFormats): Buf = {
      val requestCompressionFormat = compressionFormats.request
      val responseCompressionFormat = compressionFormats.response
      Buf.Utf8(
        s"${requestCompressionFormat.getOrElse("")};${responseCompressionFormat.getOrElse("")}")
    }

    private[this] def filterOutEmptyString(format: String): Option[String] =
      if (format.isEmpty) None else Some(format)

    /**
     * Decodes the header that the server returns to the client about which
     * compression formats to use.
     */
    def decode(encoded: Buf): CompressionFormats = {
      val string = Buf.decodeString(encoded, UTF_8)
      val idx = string.indexOf(';')
      if (idx == -1) {
        throw new IllegalArgumentException(s"protocol was encoded in the incorrect format: $string")
      }

      val requestFormat = filterOutEmptyString(string.substring(0, idx))
      val responseFormat = filterOutEmptyString(string.substring(idx + 1, string.length))

      CompressionFormats(requestFormat, responseFormat)
    }
  }

  /**
   * This is only called from the server's perspective, because the server just
   * tells the client which compression format to use.
   */
  private[finagle] def negotiate(
    serverPreferences: Compression.LocalPreferences,
    clientPreferences: Compression.PeerPreferences
  ): CompressionFormats = CompressionFormats(
    request = negotiateFormat(serverPreferences.decompression, clientPreferences.compression),
    response = negotiateFormat(serverPreferences.compression, clientPreferences.decompression)
  )

  /**
   * Used by the server to decide whether and how to compress a stream.
   *
   * The server only starts compression if neither party sets the level to `Off`
   * and at least one of them uses `Desired`.  If only one party specifies
   * `Desired`, it goes through their list of desired compression formats in
   * order, and selects the first one that the other party can also speak.
   * If both parties specify `Desired`, it uses the same process, as if the
   * server was the one party that specified `Desired`.
   *
   * @param serverSetting the server's preferences for the stream
   * @param clientSetting the client's preferences for the stream
   *
   * @return the negotiated format, or None if it will not compress the stream
   */
  private[this] def negotiateFormat(
    serverSetting: Compression.LocalSetting,
    clientSetting: Compression.PeerSetting
  ): Option[String] = {
    val clientFormats = clientSetting.transformerNames
    val serverFormats = serverSetting.transformers
    (serverSetting.level, clientSetting.level) match {
      case (CompressionLevel.Off, _) => None
      case (_, CompressionLevel.Off) => None
      case (CompressionLevel.Desired, _) =>
        serverFormats.collectFirst {
          case transformer if clientFormats.contains(transformer.name) => transformer.name
        }
      case (_, CompressionLevel.Desired) =>
        clientFormats.collectFirst {
          case name if serverFormats.exists(_.name == name) => name
        }
      case _ => None
    }
  }

  /**
   * Is unable to use compression for either the request or the response.
   */
  val CompressionOff: CompressionFormats = CompressionFormats(None, None)
}
