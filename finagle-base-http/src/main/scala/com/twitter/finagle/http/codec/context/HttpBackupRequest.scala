package com.twitter.finagle.http.codec.context

import com.twitter.finagle.context.BackupRequest
import com.twitter.util.{Try, Throw}

private object HttpBackupRequest extends HttpContext {

  type ContextKeyType = BackupRequest
  val key = BackupRequest.Ctx

  def toHeader(backupRequests: BackupRequest): String = "1"

  def fromHeader(header: String): Try[BackupRequest] = {
    header match {
      case "1" => BackupRequest.ReturnBackupRequest
      case _ =>
        Throw(
          new IllegalArgumentException(
            "Could not extract BackupRequest from Buf. Expected \"1\""
          )
        )
    }
  }
}
