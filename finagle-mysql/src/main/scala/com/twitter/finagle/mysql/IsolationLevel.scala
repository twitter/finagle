package com.twitter.finagle.mysql

sealed trait IsolationLevel {
  private[mysql] val name: String
}

/**
 * MySQL Isolation Levels. Used to override the SESSION or GLOBAL isolation level when
 * executing transactions.
 *
 * @see [[com.twitter.finagle.mysql.Transactions]]
 * @see [[https://dev.mysql.com/doc/refman/5.7/en/innodb-transaction-isolation-levels.html]]
 */
object IsolationLevel {

  case object ReadUncommitted extends IsolationLevel {
    val name = "READ UNCOMMITTED"
  }

  case object ReadCommitted extends IsolationLevel {
    val name = "READ COMMITTED"
  }

  case object RepeatableRead extends IsolationLevel {
    val name = "REPEATABLE READ"
  }

  case object Serializable extends IsolationLevel {
    val name = "SERIALIZABLE"
  }

}
