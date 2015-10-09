package com.twitter.finagle.ssl

import javax.net.ssl.SSLEngine

case class Engine(self: SSLEngine, handlesRenegotiation: Boolean = false, certId: String = "<unknown>")