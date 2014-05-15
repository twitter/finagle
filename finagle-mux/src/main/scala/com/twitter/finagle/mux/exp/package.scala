package com.twitter.finagle.mux

package object exp {
  type SessionFactory = MuxService => Session
  type SessionHandler = SessionFactory => Unit
}
