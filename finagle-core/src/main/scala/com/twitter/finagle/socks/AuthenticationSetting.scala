package com.twitter.finagle.socks

sealed abstract class AuthenticationSetting(val typeByte: Byte)
case object Unauthenticated extends AuthenticationSetting(0x00)
case class UsernamePassAuthenticationSetting(username: String, password: String)
    extends AuthenticationSetting(0x02)
