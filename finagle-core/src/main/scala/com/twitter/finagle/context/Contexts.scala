package com.twitter.finagle.context

/**
 * [[com.twitter.finagle.context.Context]]s that are managed by Finagle.
 */
object Contexts {
  /**
   * Local contexts have lifetimes bound by Finagle server requests.
   * They are local to the process.
   */
  val local: LocalContext = new LocalContext
  
  /**
   * Broadcast contexts may be marshalled and transmitted across
   * process boundaries. Finagle clients typically marshal the
   * current context state for outbound requests; Finagle servers
   * receive marshalled contexts and restore them before dispatching
   * a new request.
   *
   * Thus broadcast contexts are transmitted throughout an entire
   * request tree, so long as the protocols involved support
   * marshalled context values.
   */
  val broadcast: MarshalledContext = new MarshalledContext
}
