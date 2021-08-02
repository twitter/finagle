package com.twitter.finagle

private[finagle] object IOExceptionStrings {

  /** Strings that commonly signal a broken socket connection */
  val ChannelClosedStrings: Set[String] = Set(
    "Connection reset by peer", // Found on linux
    "Broken pipe", // Found on linux
    "An existing connection was forcibly closed by the remote host", // Found on windows
    "syscall:read(..) failed: Connection reset by peer", // Found on linux w/ native epoll
    "readAddress(..) failed: Connection reset by peer", // Found on linux w/ native epoll
    "writeAddress(..) failed: Connection reset by peer", // Found on linux w/ native epoll
    "writevAddresses(..) failed: Connection reset by peer", // Found on linux w/ native epoll
    "writevAddresses(..) failed: Broken pipe" // Found on linux w/ native epoll
  )

  /** Strings that commonly signal failure to establish a socket connection */
  val ConnectionFailedStrings: Set[String] = Set(
    "Connection timed out", // from ConnectionFailedException found on linux NIO1
    "No route to host"
  )

  /** Exception strings that are common for `IOException`s that don't need vocal logging */
  val FinestIOExceptionMessages: Set[String] = ChannelClosedStrings ++ ConnectionFailedStrings

  /** SSLException strings that mean the channel has been closed */
  val ChannelClosedSslExceptionMessages: Set[String] = Set("SSLEngine closed already")
}
