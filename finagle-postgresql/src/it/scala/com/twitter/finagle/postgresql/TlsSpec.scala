package com.twitter.finagle.postgresql

import java.nio.file.Files
import java.nio.file.attribute.PosixFilePermission

import com.spotify.docker.client.messages.HostConfig
import com.twitter.finagle.PostgreSql
import com.twitter.finagle.ssl.TrustCredentials
import com.twitter.finagle.ssl.client.SslClientConfiguration
import com.whisk.docker.testkit.ContainerSpec

import scala.jdk.CollectionConverters._

class TlsSpec extends PgSqlIntegrationSpec with ResourceFileSpec {

  /**
   * Here be dragons.
   *
   * A Docker mount of type "bind" will have the uid:gid of the host user inside the container.
   * For example, if the host user running `docker run` is `1001:116`, the mounted file in the container will be owned by `1001:116`.
   *
   * For the TLS private key file, postgres will only accept reading it if it is owned by root or the user running postgres.
   * Furthermore, it will check that the permissions are not "world readable".
   *
   * The 2 statements above makes it difficult to provide a private key to postgres: the host user does not exist in the container
   * yet, it must run own the secret key AND run postgres.
   *
   * The solution used is to run postgres as the host user, but this requires the following workarounds:
   *
   *   * run the container as the host's `uid:gid`
   *   * mount the host `/etc/passwd` as `/etc/passwd` in the container so the host user exists
   *   * use a subdirectory of the default `PGDATA` value so `initdb` can successfully do its thing
   *
   * When necessary, the host user's `uid` and `gid` must be provided using the `CI_UID_GID` environment variable and should
   * be formatted as `"uid:gid"` (without the double quotes).
   *
   * NOTE: on OSX none of this is necessary, for some reason, the mounted files are owned by root.
   */
  def runAs = Option(System.getenv("CI_UID_GID"))

  def passwdVolume = runAs.map { _ =>
    HostConfig.Bind.builder()
      .from("/etc/passwd")
      .to("/etc/passwd")
      .readOnly(true)
      .build()
  }

  def keyPerms(file: java.io.File) =
    Files.setPosixFilePermissions(
      file.toPath,
      Set(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE).asJava
    ).toFile

  override def postgresContainerEnv = Map(
    "PGDATA" -> "/var/lib/postgresql/data/pg_data"
  )

  def tlsVolumes = List(
    HostConfig.Bind.builder()
      .from(toTmpFile("/server.crt").getAbsolutePath)
      .to("/certs/server.crt")
      .readOnly(true)
      .build(),
    HostConfig.Bind.builder()
      .from(keyPerms(toTmpFile("/server.key")).getAbsolutePath)
      .to("/certs/server.key")
      .readOnly(true)
      .build()
  )

  override def configure(spec: ContainerSpec): ContainerSpec =
    spec
      .withOption(runAs)((s, user) => s.withConfiguration(_.user(user)))
      .withVolumeBindings((passwdVolume.toList ++ tlsVolumes): _*)
      .withCommand(
        "-c",
        "ssl=true",
        "-c",
        "ssl_cert_file=/certs/server.crt",
        "-c",
        "ssl_key_file=/certs/server.key",
      )

  def withTls(client: PostgreSql.Client): PostgreSql.Client =
    client.withTransport.tls(SslClientConfiguration(trustCredentials = TrustCredentials.Insecure))

  "TLS" should {

    "support tls" in withClient(cfg = withTls) { client =>
      client.toService(Request.Sync)
        .map { response =>
          response must beEqualTo(Response.Ready)
        }
    }
  }
}

class MissingTlsSpec extends PgSqlIntegrationSpec with ResourceFileSpec {

  def withTls(client: PostgreSql.Client): PostgreSql.Client =
    client.withTransport.tls(SslClientConfiguration(trustCredentials = TrustCredentials.Insecure))

  "TLS" should {
    "handle unsupported tls" in withClient(cfg = withTls) { client =>
      client.toService(Request.Sync)
        .liftToTry
        .map { response =>
          response.asScala must beAFailedTry(beEqualTo(PgSqlTlsUnsupportedError))
        }
    }
  }
}
