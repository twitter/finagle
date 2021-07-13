package com.twitter.finagle.mysql.integration

import com.twitter.finagle.Mysql
import com.twitter.finagle.mysql.HandshakeStackModifier
import com.twitter.finagle.mysql.harness.EmbeddedSuite
import com.twitter.finagle.mysql.harness.config.{DatabaseConfig, InstanceConfig, MySqlVersion, User}
import com.twitter.finagle.mysql.param.CachingSha2PasswordMissServerCache
import com.twitter.finagle.stats.InMemoryStatsReceiver

class CachingSha2PasswordAuthTest extends EmbeddedSuite {
  val aUser: User = User("aUser", Some("aPassword"), User.Permission.All)
  val noPwUser: User = User("noPwUser", None, User.Permission.All)

  val basePrivateKey = "/auth/keys/mysql_rsa_private_key.pem"
  val basePublicKey = "/auth/keys/mysql_rsa_public_key.pem"

  val cachingSha2PasswordAuthParameters: Map[String, String] = Map(
    "--default-authentication-plugin" -> "caching_sha2_password",
    "--sha256-password-private-key-path" -> getClass.getResource(basePrivateKey).getPath,
    "--caching-sha2-password-private-key-path" -> getClass.getResource(basePrivateKey).getPath,
    "--sha256-password-public-key-path" -> getClass.getResource(basePublicKey).getPath,
    "--caching-sha2-password-public-key-path" -> getClass.getResource(basePublicKey).getPath
  )

  val mysql8: MySqlVersion = v8_0_21.copy(serverStartParameters =
    v8_0_21.serverStartParameters ++ SslTestUtils.getSslParametersAsMap ++ cachingSha2PasswordAuthParameters)

  val instanceConfig: InstanceConfig = InstanceConfig(mysql8)
  val databaseConfig: DatabaseConfig = defaultDatabaseConfig.copy(
    databaseName = "cachingSha2PasswordAuth",
    users = Seq(
      aUser,
      noPwUser
    )
  )

  private def configureTlsEnabledClient(
    client: Mysql.Client,
    causeCacheMiss: Boolean,
    sr: InMemoryStatsReceiver
  ): Mysql.Client = {
    client.withCachingSha2Password
      .withStatsReceiver(sr)
      .configured(CachingSha2PasswordMissServerCache(causeCacheMiss))
      .configured(HandshakeStackModifier(SslTestUtils.downgradedHandshake))
      .withTransport.tls
  }

  private def configureUnsecuredClient(
    client: Mysql.Client,
    rsaKeyPath: String,
    causeCacheMiss: Boolean,
    sr: InMemoryStatsReceiver
  ): Mysql.Client = {
    client.withCachingSha2Password
      .withStatsReceiver(sr)
      .withServerRsaPublicKey(rsaKeyPath)
      .configured(CachingSha2PasswordMissServerCache(causeCacheMiss))
  }

  private def selectFastAuthSuccessFromGauges(sr: InMemoryStatsReceiver): Int = {
    val stat = "fast_auth_success"
    sr.counters.keys.find(strs => strs.contains(stat)) match {
      case Some(key) => sr.counters(key).toInt
      case None => throw new IllegalStateException(s"Missing stat '$stat'")
    }
  }

  // Empty password
  test("empty password full auth with tls") { fixture =>
    val sr = new InMemoryStatsReceiver()
    val client =
      configureTlsEnabledClient(fixture.newClient(user = noPwUser), causeCacheMiss = true, sr = sr)
        .newRichClient(fixture.instance.dest)
    await(client.ping())
    assert(selectFastAuthSuccessFromGauges(sr) == 0)
  }

  test("empty password full auth over plaintext") { fixture =>
    val sr = new InMemoryStatsReceiver()
    val client =
      configureUnsecuredClient(
        fixture.newClient(user = noPwUser),
        rsaKeyPath = "",
        causeCacheMiss = true,
        sr = sr)
        .newRichClient(fixture.instance.dest)
    await(client.ping())
    assert(selectFastAuthSuccessFromGauges(sr) == 0)
  }

  // Non-empty password
  test("non-empty password full auth with tls") { fixture =>
    val sr = new InMemoryStatsReceiver()
    val client =
      configureTlsEnabledClient(fixture.newClient(user = aUser), causeCacheMiss = true, sr = sr)
        .newRichClient(fixture.instance.dest)
    await(client.ping())
    assert(selectFastAuthSuccessFromGauges(sr) == 0)
  }

  // This test depends on the previous test since fast auth can only succeed
  // if the same user has authenticated previously. The password cache on the server
  // does not survive a server restart.
  test("non-empty password fast auth with tls") { fixture =>
    val sr = new InMemoryStatsReceiver()
    val client =
      configureTlsEnabledClient(fixture.newClient(user = aUser), causeCacheMiss = false, sr = sr)
        .newRichClient(fixture.instance.dest)
    await(client.ping())
    assert(selectFastAuthSuccessFromGauges(sr) == 1)
  }

  test("non-empty password full auth over plaintext, request RSA public key from server") {
    fixture =>
      val sr = new InMemoryStatsReceiver()
      val client =
        configureUnsecuredClient(
          fixture.newClient(user = aUser),
          rsaKeyPath = "",
          causeCacheMiss = true,
          sr = sr)
          .newRichClient(fixture.instance.dest)
      await(client.ping())
      assert(selectFastAuthSuccessFromGauges(sr) == 0)
  }

  // This test depends on the previous test since fast auth can only succeed
  // if the same user has authenticated previously. The password cache on the server
  // does not survive a server restart.
  test("non-empty password fast auth over plaintext") { fixture =>
    val sr = new InMemoryStatsReceiver()
    val client =
      configureUnsecuredClient(
        fixture.newClient(user = aUser),
        rsaKeyPath = "",
        causeCacheMiss = false,
        sr = sr)
        .newRichClient(fixture.instance.dest)
    await(client.ping())
    assert(selectFastAuthSuccessFromGauges(sr) == 1)
  }

  test("non-empty password full auth over plaintext with locally stored RSA public key") {
    fixture =>
      val sr = new InMemoryStatsReceiver()
      val rsaPublicKeyPath = getClass.getResource("/auth/keys/mysql_rsa_public_key.pem").getPath
      val client =
        configureUnsecuredClient(
          fixture.newClient(user = aUser),
          rsaKeyPath = rsaPublicKeyPath,
          causeCacheMiss = true,
          sr = sr)
          .newRichClient(fixture.instance.dest)
      await(client.ping())
      assert(selectFastAuthSuccessFromGauges(sr) == 0)
  }
}
