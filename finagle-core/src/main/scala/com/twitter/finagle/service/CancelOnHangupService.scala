package com.twitter.finagle.service

import com.twitter.finagle.{Service, ServiceProxy}
import com.twitter.util.Future
import scala.collection.mutable

class CancelOnHangupService[Req, Rep](self: Service[Req, Rep])
  extends ServiceProxy[Req, Rep](self)
{
  private[this] val pending = new mutable.HashSet[Future[Rep]]
    with mutable.SynchronizedSet[Future[Rep]]

  override def apply(req: Req): Future[Rep] = {
    val f = super.apply(req)
    pending.add(f)
    f ensure { pending.remove(f) }
  }

  override def release() {
    // We force a buffer here because we could
    // otherwise produce a deadlock since foreach
    // is synchronized.
    pending.toArray foreach { f => f.cancel() }
    pending.clear()
    super.release()
  }
}