package com.twitter.finagle

/**
 * Define a transaction {@link Local} that stores the current
 * transaction ID.
 */

import scala.util.Random

import com.twitter.util.Local

object Transaction {
  private[this] val rng = new Random
  private[this] val transactionID = new Local[Long]

  def set(id: Long) {
    transactionID() = id
  }

  def get() = {
    if (!transactionID().isDefined)
      newTransaction()

    transactionID().get
  }

  def newTransaction() {
    transactionID() = rng.nextLong()
  }
}
