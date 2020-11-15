package com.twitter.finagle.postgresql

import com.twitter.finagle.PostgreSql
import com.twitter.finagle.Service
import com.twitter.finagle.ServiceFactory
import org.specs2.execute.AsResult
import org.specs2.execute.Result

trait ClientEach { _: PostgresConnectionSpec =>

  type ClientCfg = PostgreSql.Client => PostgreSql.Client

  def withClient[R: AsResult](
    config: ConnectionCfg = defaultConnectionCfg,
    cfg: ClientCfg = identity
  )(spec: ServiceFactory[Request, Response] => R): Result = {
    val client = cfg(
      PostgreSql.Client()
        .withCredentials(config.username, config.password)
        .withDatabase(config.database)
    ).newClient(s"${config.host}:${config.port}")

    AsResult(spec(client))
  }

  def withRichClient[R: AsResult](
    config: ConnectionCfg = defaultConnectionCfg,
    cfg: ClientCfg = identity
  )(spec: Client => R): Result =
    withClient(config, cfg) { client =>
      spec(Client(client))
    }

  def withService[R: AsResult](
    config: ConnectionCfg = defaultConnectionCfg,
    cfg: ClientCfg = identity
  )(spec: Service[Request, Response] => R): Result =
    withClient(config, cfg) { client =>
      spec(client.toService)
    }

}
