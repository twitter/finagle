package com.twitter.finagle.postgresql

import com.twitter.util.Await
import com.twitter.util.Future
import org.specs2.execute.AsResult

trait FutureResult {
  implicit def futureAsResult[T: AsResult]: AsResult[Future[T]] =
    AsResult.asResult(f => AsResult(Await.result(f)))
}
