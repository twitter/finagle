package com.twitter.finagle.postgresql

import com.twitter.finagle.PostgreSql
import com.twitter.finagle.Service
import com.twitter.finagle.ServiceFactory

trait ClientEach { _: PostgresConnectionSpec =>

  type ClientCfg = PostgreSql.Client => PostgreSql.Client

  def withClient[R](
    config: ConnectionCfg = defaultConnectionCfg,
    cfg: ClientCfg = identity
  )(spec: ServiceFactory[Request, Response] => R): R = {
    val client = cfg(
      PostgreSql.Client()
        .withCredentials(config.username, config.password)
        .withDatabase(config.database)
    ).newClient(s"${config.host}:${config.port}")

    spec(client)
  }

  def withRichClient[R](
    config: ConnectionCfg = defaultConnectionCfg,
    cfg: ClientCfg = identity
  )(spec: Client => R): R =
    withClient(config, cfg) { client =>
      spec(Client(client))
    }

  def withService[R](
    config: ConnectionCfg = defaultConnectionCfg,
    cfg: ClientCfg = identity
  )(spec: Service[Request, Response] => R): R =
    withClient(config, cfg) { client =>
      spec(client.toService)
    }

}
