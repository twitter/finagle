package com.twitter.finagle.mysql.param

import com.twitter.finagle.mysql.MysqlCharset.Utf8_general_ci
import com.twitter.finagle.Stack
import com.twitter.finagle.mysql.Request

/**
 * A class eligible for configuring a mysql client's credentials during
 * the Handshake phase.
 */
case class Credentials(username: Option[String], password: Option[String])
object Credentials {
  implicit val param: Stack.Param[Credentials] = new Stack.Param[Credentials] {
    val default: Credentials = Credentials(None, None)

    override def show(p: Credentials): Seq[(String, () => String)] = {
      // do not show the password for security reasons
      Seq(("username", () => p.username.getOrElse("")))
    }
  }
}

/**
 * A class eligible for configuring a mysql client's database during
 * the Handshake phase.
 */
case class Database(db: Option[String])
object Database {
  implicit val param: Stack.Param[Database] = Stack.Param(Database(None))
}

/**
 * A class eligible for configuring a mysql client's charset during
 * the Handshake phase.
 */
case class Charset(charset: Short)
object Charset {
  implicit val param: Stack.Param[Charset] = Stack.Param(Charset(Utf8_general_ci))
}

/**
 * A class eligible for configuring a mysql client's CLIENT_FOUND_ROWS flag
 * during the Handshake phase.
 */
case class FoundRows(enabled: Boolean)
object FoundRows {
  implicit val param: Stack.Param[FoundRows] = Stack.Param(FoundRows(true))
}

/**
 * A class eligible for configuring a mysql client's CLIENT_INTERACTIVE flag
 * during the Handshake phase. If the client is interactive,
 * System_variables::net_interactive_timeout is used for the wait_timeout. If the client is not
 * interactive, System_variables::net_wait_timeout is used.
 */
case class Interactive(enabled: Boolean)
object Interactive {
  implicit val param: Stack.Param[Interactive] = Stack.Param(Interactive(true))
}

/**
 * A class eligible for configuring the maximum number of prepare
 * statements.  After creating `num` prepare statements, we'll start purging
 * old ones.
 */
case class MaxConcurrentPrepareStatements(num: Int) {
  assert(num <= Int.MaxValue, s"$num is not <= Int.MaxValue bytes")
  assert(num > 0, s"$num must be positive")

  def mk(): (MaxConcurrentPrepareStatements, Stack.Param[MaxConcurrentPrepareStatements]) =
    (this, MaxConcurrentPrepareStatements.param)
}

object MaxConcurrentPrepareStatements {
  implicit val param: Stack.Param[MaxConcurrentPrepareStatements] =
    Stack.Param(MaxConcurrentPrepareStatements(20))
}

/**
 * Configure whether to support unsigned integer fields when returning elements of a [[Row]].
 * If not supported, unsigned fields will be decoded as if they were signed, potentially
 * resulting in corruption in the case of overflowing the signed representation. Because
 * Java doesn't support unsigned integer types widening may be necessary to support the
 * unsigned variants. For example, an unsigned Int is represented as a Long.
 *
 * `Value` representations of unsigned columns which are widened when enabled:
 * `ByteValue` -> `ShortValue``
 * `ShortValue` -> IntValue`
 * `LongValue` -> `LongLongValue`
 * `LongLongValue` -> `BigIntValue`
 */
case class UnsignedColumns(supported: Boolean)
object UnsignedColumns {
  implicit val param: Stack.Param[UnsignedColumns] = Stack.Param(UnsignedColumns(false))
}

/**
 * A class eligible for configuring a initial request which used when establishing a new session.
 */
case class ConnectionInitRequest(request: Option[Request])
object ConnectionInitRequest {
  implicit val param: Stack.Param[ConnectionInitRequest] = Stack.Param(ConnectionInitRequest(None))
}

/**
 * To configure the client to use the caching_sha2_password authentication method.
 * Needed to set the CLIENT_PLUGIN_AUTH capability flag during the handshake phase.
 */
final case class CachingSha2PasswordAuth(enabled: Boolean)
object CachingSha2PasswordAuth {
  implicit val param: Stack.Param[CachingSha2PasswordAuth] =
    Stack.Param(CachingSha2PasswordAuth(false))
}

/**
 * To configure the local path to the server's RSA public key. This is used during
 * caching_sha2_password authentication when authenticating over a plaintext connection.
 */
final case class PathToServerRsaPublicKey(path: String)
object PathToServerRsaPublicKey {
  implicit val param: Stack.Param[PathToServerRsaPublicKey] =
    Stack.Param(PathToServerRsaPublicKey(""))
}

/**
 * To cause a cache miss in the server's password cache to ensure that we are using
 * full auth during authentication. Used for testing.
 */
private[finagle] final case class CachingSha2PasswordMissServerCache(causeMiss: Boolean)
private[finagle] object CachingSha2PasswordMissServerCache {
  implicit val param: Stack.Param[CachingSha2PasswordMissServerCache] =
    Stack.Param(CachingSha2PasswordMissServerCache(false))
}
