package com.twitter.finagle.mysql.harness.config

sealed trait MySqlUserType {
  def value: String
}

object MySqlUserType {

  case object RW extends MySqlUserType {
    override val value: String = "ALL"
  }
  case object RO extends MySqlUserType {
    override val value: String = "SELECT"
  }
}
