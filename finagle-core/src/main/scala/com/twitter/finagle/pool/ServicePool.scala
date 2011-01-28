package com.twitter.finagle.pool

import com.twitter.util.Pool

import com.twitter.finagle.Service

trait DrainablePool[A] extends Pool[A] {
  def drain(): Unit
}

trait ServicePool[Req, Rep] extends DrainablePool[Service[Req, Rep]]
