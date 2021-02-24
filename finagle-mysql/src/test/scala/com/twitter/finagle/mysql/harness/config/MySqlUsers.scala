package com.twitter.finagle.mysql.harness.config

case class MySqlDBUsers(rwUser: RWUser, roUser: ROUser)

trait MySqlUser {
  val name: String
  val password: Option[String]
  val userType: MySqlUserType
}

case class RWUser(name: String,  password: Option[String]) extends MySqlUser {
  override val userType: MySqlUserType = MySqlUserType.RW
} 
case class ROUser(name: String,  password: Option[String]) extends MySqlUser {
  override val userType: MySqlUserType = MySqlUserType.RO
}
