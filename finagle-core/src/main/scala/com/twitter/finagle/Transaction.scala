package com.twitter.finagle

/**
 * Define a transaction {@link Local} that stores the current
 * transaction ID.
 */

import scala.util.Random

import com.twitter.util.Local

case class Transaction(id: Long, payload: Array[Byte])

object Transaction {
  private[this] val rng = new Random
  private[this] val current = new Local[Transaction]

  def update(transaction: Transaction) {
    current() = transaction
  }

  def apply() = {
    if (!current().isDefined)
      newTransaction()

    current().get
  }

  def newTransaction() {
    current() = Transaction(rng.nextLong(), Array[Byte]())
  }
}
