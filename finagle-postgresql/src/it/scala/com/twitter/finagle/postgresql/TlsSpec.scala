package com.twitter.finagle.postgresql

import java.nio.file.Files
import java.nio.file.attribute.PosixFilePermission

import com.twitter.finagle.postgresql.Response.BackendResponse
import com.twitter.finagle.ssl.TrustCredentials
import com.twitter.finagle.ssl.client.SslClientConfiguration
import com.twitter.io.StreamIO
import com.twitter.io.TempFile
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres

import scala.collection.JavaConverters._

class TlsSpec extends PgSqlSpec with EmbeddedPgSqlSpec {

  def using[I <: java.io.Closeable, T](io: => I)(f: I => T) = {
    val c = io
    try { f(c) } finally { c.close() }
  }
  def toTmpFile(name: String) = {
    using(getClass.getResourceAsStream(name)) { is =>
      val file = TempFile.fromResourcePath(name)
      Files.setPosixFilePermissions(file.toPath, Set(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE).asJava)
      using(new java.io.FileOutputStream(file)) { os =>
        StreamIO.copy(is, os)
        file
      }
    }
  }

  override def configure(builder: EmbeddedPostgres.Builder): EmbeddedPostgres.Builder = {
    builder
      .setServerConfig("ssl", "true")
      .setServerConfig("ssl_cert_file", toTmpFile("/server.crt").getAbsolutePath)
      .setServerConfig("ssl_key_file", toTmpFile("/server.key").getAbsolutePath)
  }

  "TLS" should {
    "support tls" in {
      client(_.withTransport.tls(SslClientConfiguration(trustCredentials = TrustCredentials.Insecure)))
        .apply(Request.Sync)
        .map { response =>
          response must beEqualTo(BackendResponse(BackendMessage.ReadyForQuery(BackendMessage.NoTx)))
        }
    }
  }

}
