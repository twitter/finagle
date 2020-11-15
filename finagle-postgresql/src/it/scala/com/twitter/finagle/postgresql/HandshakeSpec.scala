package com.twitter.finagle.postgresql

import com.spotify.docker.client.messages.HostConfig
import com.whisk.docker.testkit.ContainerSpec

class HandshakeSpec extends PgSqlIntegrationSpec with ResourceFileSpec {

  override def configure(spec: ContainerSpec): ContainerSpec = {
    spec
      .withVolumeBindings(
        HostConfig.Bind.builder()
          .from(toTmpFile("/handshake_pg_hba.conf").getAbsolutePath)
          .to("/handshake/pg_hba.conf")
          .build()
      )
      .withCommand("-c", "hba_file=/handshake/pg_hba.conf")
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    val _ = withStatement() { stmt =>
      stmt.execute("CREATE USER clear_user WITH PASSWORD 'not secret'")
      stmt.execute("CREATE USER md5_user WITH PASSWORD 'super secret'")
    }
  }

  "Handshake" should {

    def authSpec(username: String, password: Option[String]) =
      withClient(cfg = _.withCredentials(username, password)) { client =>
        client
          .toService(Request.ConnectionParameters)
          .map { response =>
            response must beLike {
              case params: Response.ConnectionParameters =>
                params.parameterMap(BackendMessage.Parameter.SessionAuthorization) must_== username
            }
          }
      }

    "support password-less authentication" in {
      authSpec("postgres", None)
    }

    "support clear text authentication" in {
      authSpec("clear_user", Some("not secret"))
    }

    "support md5 authentication" in {
      authSpec("md5_user", Some("super secret"))
    }
  }

}
