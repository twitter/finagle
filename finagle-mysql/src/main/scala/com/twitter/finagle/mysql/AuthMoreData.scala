package com.twitter.finagle.mysql

/**
 * Used to differentiate information sent in the AuthMoreData packet
 * and the various cycles during authentication.
 *
 * There are three types of AuthMoreData packets:
 *  1. Request public key
 *  2. Fast authentication success
 *  3. Perform full authentication
 *
 * @see: [[https://github.com/mysql/mysql-server/blob/7ed30a748964c009d4909cb8b4b22036ebdef239/sql/auth/sha2_password.cc#L779-L781]]
 */
private[mysql] abstract class AuthMoreDataType {
  def moreDataByte: Byte
}

private[mysql] object NeedPublicKey extends AuthMoreDataType {
  val moreDataByte: Byte = 0x02
}

private[mysql] object FastAuthSuccess extends AuthMoreDataType {
  val moreDataByte: Byte = 0x03
}

private[mysql] object PerformFullAuth extends AuthMoreDataType {
  val moreDataByte: Byte = 0x04
}
