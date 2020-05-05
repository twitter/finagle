package com.twitter.finagle.ssl

import com.twitter.finagle.ssl.TlsSnooping.DetectionResult
import com.twitter.io.{Buf, ByteReader}

/**
 * Collection of default TLS snooping implementations.
 *
 * Currently only TLS 1.x is supported.
 */
private object Snoopers {

  // Copied from `Netty4.SslUtils`
  private val SslRecordHeaderLength = 5
  private val SslContentTypeHandshake = 22

  def tls12Detector(buf: Buf): DetectionResult = {
    // Make sure we have enough data to do our analysis
    if (buf.length < SslRecordHeaderLength) {
      return DetectionResult.NeedMoreData
    }

    val reader = ByteReader(buf)

    // We're looking at a TLS record. Its structure is
    // https://tools.ietf.org/html/rfc5246#section-6.2.1
    //
    // struct {
    //     uint8 major;
    //     uint8 minor;
    // } ProtocolVersion;
    //
    // enum {
    //     change_cipher_spec(20), alert(21), handshake(22),
    //     application_data(23), (255)
    // } ContentType;
    //
    // struct {
    //     ContentType type;
    //     ProtocolVersion version;
    //     uint16 length;
    //     opaque fragment[TLSPlaintext.length];
    // } TLSPlaintext;
    //
    // Since we're only interested in the initial bytes we must see a type
    // `handshake` and then the major version. We should also have a sane length
    //
    // SSLv3 or TLS - Check ContentType
    if (reader.readByte() != SslContentTypeHandshake) {
      return DetectionResult.Cleartext
    }

    // Note that we don't check the minor version since it can fluctuate based on implementation
    val majorVersion = reader.readByte()
    if (majorVersion != 3) {
      return DetectionResult.Cleartext
    }

    // Burn a byte for the TLS minor version which we don't consider
    reader.skip(1)

    // We're probably either SSLv3 or TLS
    // The length must be be non-negative and greater than 0 for a
    // handshake record and all records must not exceed 2^14.
    val packetLength = reader.readShortBE()
    if (0 < packetLength && packetLength <= (1 << 14)) DetectionResult.Secure
    else DetectionResult.Cleartext
  }

}
