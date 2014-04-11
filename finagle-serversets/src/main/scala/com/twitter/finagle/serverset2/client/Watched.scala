package com.twitter.finagle.serverset2.client

import com.twitter.util.Var

private[serverset2] case class Watched[+T](value: T, state: Var[WatchState])
