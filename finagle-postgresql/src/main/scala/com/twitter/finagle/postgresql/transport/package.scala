package com.twitter.finagle.postgresql

import com.twitter.finagle.transport.Transport

package object transport {
  type ClientTransport = Transport[FrontendMessage, BackendMessage] {
    type Context <: PgTransportContext
  }
}
