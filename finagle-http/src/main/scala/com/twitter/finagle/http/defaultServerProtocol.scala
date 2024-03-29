package com.twitter.finagle.http

import com.twitter.app.GlobalFlag

/**
 * [[GlobalFlag]] that can be used to configure the default protocol used when creating a new
 * [[com.twitter.finagle.Http.Server]].
 */
object defaultServerProtocol
    extends GlobalFlag[Protocol](Protocol.Default, "Default HTTP Server Protocol")
