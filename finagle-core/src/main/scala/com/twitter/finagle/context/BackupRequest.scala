package com.twitter.finagle.context
import com.twitter.io.Buf
import com.twitter.util.{Return, Throw, Try}

private[finagle] final class BackupRequest private ()

/**
 * Used to signal that a request was initiated as a "backup request".
 *
 * Note that these are broadcast across the rest of the request's call graph.
 *
 * @see [[com.twitter.finagle.client.BackupRequestFilter]] for details on
 *     what a backup request is.
 * @see MethodBuilder's `idempotent` for configuring a client to use backups.
 */
object BackupRequest {

  private[finagle] val ContextId: String = "com.twitter.finagle.BackupRequest"

  private[this] val backupRequest = new BackupRequest
  private[finagle] val ReturnBackupRequest = Return(backupRequest)

  private[this] final class Context extends Contexts.broadcast.Key[BackupRequest](ContextId) {
    def marshal(value: BackupRequest): Buf = Buf.Empty

    def tryUnmarshal(buf: Buf): Try[BackupRequest] = {
      if (buf.isEmpty)
        ReturnBackupRequest
      else
        Throw(
          new IllegalArgumentException(
            s"Could not extract BackupRequest from Buf. Length ${buf.length} but required 0"
          ))
    }
  }

  private[finagle] val Ctx: Contexts.broadcast.Key[BackupRequest] = new Context

  /**
   * Whether or not a request was initiated as a backup request.
   */
  def wasInitiated: Boolean = Contexts.broadcast.contains(Ctx)

  /**
   * Execute `fn` such that calls to [[wasInitiated]] will return `true` both
   * here and across it's RPC call graph.
   */
  private[finagle] def let[T](fn: => T): T = Contexts.broadcast.let(Ctx, backupRequest) {
    fn
  }

}
