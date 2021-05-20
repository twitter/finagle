package com.twitter.finagle.mux.transport

import com.twitter.finagle.mux.transport.Compression._
import com.twitter.finagle.mux.transport.CompressionNegotiation._
import com.twitter.io.Buf
import org.scalatest.funsuite.AnyFunSuite

class CompressionNegotiationTest extends AnyFunSuite {
  test(s"Can encode and decode preferences") {
    CompressionTest.configurations.foreach { preferences =>
      val preferencesString = preferences.toString
      val encoded = ClientHeader.encode(preferences)
      assert(ClientHeader.decode(encoded) == CompressionTest.toPeerPreferences(preferences))
    }
  }

  test(s"Can encode and decode compression formats") {
    Seq(
      CompressionFormats(None, None),
      CompressionFormats(Some("foo"), None),
      CompressionFormats(None, Some("foo")),
      CompressionFormats(Some("foo"), Some("bar"))
    ).foreach { compressionFormats =>
      val formatString = compressionFormats.toString
      val encoded = ServerHeader.encode(compressionFormats)
      assert(ServerHeader.decode(encoded) == compressionFormats)
    }
  }

  test(s"Can't make a LocalSetting with a transformer with an illegal character in the name") {
    Seq(';', ',', ':').foreach { badChar =>
      intercept[IllegalArgumentException] {
        LocalSetting(CompressionLevel.Off, Seq(createTestByteTransformer("foo" + badChar)))
      }
    }

    // just want to check we do OK for all of these
    Seq('a', 'l', 'z', 'A', 'L', 'Z', '0', '5', '9').foreach { goodChar =>
      LocalSetting(CompressionLevel.Off, Seq(createTestByteTransformer("foo" + goodChar)))
    }
  }

  private[this] def validateState(
    compressionFormat: Option[String],
    server: LocalSetting,
    client: PeerSetting
  ): Unit = compressionFormat match {
    case Some(format) =>
      assert(server.transformers.exists(_.name == format))
      assert(client.transformerNames.contains(format))
      assert(client.level != CompressionLevel.Off)
      assert(server.level != CompressionLevel.Off)
      assert(client.level == CompressionLevel.Desired || server.level == CompressionLevel.Desired)
    case None =>
      assert(
        client.level == CompressionLevel.Off ||
          server.level == CompressionLevel.Off ||
          (client.level == CompressionLevel.Accepted &&
            server.level == CompressionLevel.Accepted) ||
          server.transformers.map(_.name).intersect(client.transformerNames).isEmpty
      )
  }

  test("Compression negotiation passes some sniff tests") {
    for {
      server <- CompressionTest.configurations
      rawClient <- CompressionTest.configurations
      client = CompressionTest.toPeerPreferences(rawClient)
      result = negotiate(server, client)
    } {
      val requestCompression = result.request
      val responseCompression = result.response
      validateState(requestCompression, server.decompression, client.compression)
      validateState(responseCompression, server.compression, client.decompression)
    }
  }

  test("Fails in a forwards-compatible way for not understanding preferences") {
    assert(ClientHeader.decode(Buf.Utf8("foo")) == Compression.PeerCompressionOff)
  }

  test("Fails hard when it doesn't understand the negotiated compression formats") {
    intercept[IllegalArgumentException] {
      ServerHeader.decode(Buf.Utf8("foo"))
    }
  }
}

object CompressionTest {
  private[this] val identityTransformer = createTestByteTransformer("identity")
  private[this] val reverseTransformer = createTestByteTransformer("reverse")
  private[this] val combinations = Seq(
    Seq(identityTransformer, reverseTransformer),
    Seq(reverseTransformer, identityTransformer),
    Seq(identityTransformer),
    Seq(reverseTransformer),
    Nil
  )

  private[this] def createTests(level: CompressionLevel): Seq[LocalSetting] =
    combinations.map(LocalSetting(level, _))
  private[this] val off = createTests(CompressionLevel.Off)
  private[this] val accepting = createTests(CompressionLevel.Accepted)
  private[this] val desiring = createTests(CompressionLevel.Desired)

  private[this] val settings = off ++ accepting ++ desiring
  val configurations = for {
    left <- settings
    right <- settings
  } yield LocalPreferences(left, right)

  private[this] def toPeerSetting(local: LocalSetting): PeerSetting =
    PeerSetting(local.level, local.transformers.map(_.name))

  def toPeerPreferences(local: LocalPreferences): PeerPreferences =
    PeerPreferences(toPeerSetting(local.compression), toPeerSetting(local.decompression))
}
