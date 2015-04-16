package com.twitter.finagle.mux

import com.twitter.conversions.time._
import com.twitter.finagle._
import com.twitter.finagle.exp.TokenBucket
import com.twitter.finagle.service.{RequeueingFilter, RetryPolicy}
import com.twitter.finagle.stats.Counter
import com.twitter.util.Future

/**
 * A (for now) custom requeue module for Mux. In addition to
 * requeueing local failures, module Requeue re-issues remote
 * requests that have been NACKd. We're more careful with these: they
 * are rate limited by a token bucket at a 1:5 ratio of requests. The
 * token bucket also has a reserve of 10 reissues/s. This is to aid
 * just-starting or low-velocity clients. Thus, reissue can never
 * exceed the greater of 10/s and ~15% of total issue.
 */
private[finagle] object Requeue {
  /**
   * Effort is the maximum number of local requeues.
   */
  private val Effort = 25
  
  /**
   * Cost determines the relative cost of a reissue vs. an initial
   * issue.
   */
  private val Cost = 5

  class RequeueFilter[Req, Rep](
      bucket: TokenBucket,
      requeueCost: Int,
      counter: Counter,
      stackStatus: => Status)
    extends SimpleFilter[Req, Rep] {

    private[this] def applyService(req: Req, service: Service[Req, Rep]): Future[Rep] = {
      service(req) rescue {
        case exc@RetryPolicy.RetryableWriteException(_) =>
          if (stackStatus == Status.Open && bucket.tryGet(requeueCost)) {
            counter.incr()
            applyService(req, service)
          } else {
            Future.exception(exc)
          }
      }
    }

    def apply(req: Req, service: Service[Req, Rep]): Future[Rep] = {
      bucket.put(1)
      applyService(req, service)
    }
  }

  def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module1[param.Stats, ServiceFactory[Req, Rep]] {
      val role = RequeueingFilter.role
      val description = "Requeue requests that have been rejected"

      def make(stats: param.Stats, next: ServiceFactory[Req, Rep]) = {
        val param.Stats(sr0) = stats
        val sr = sr0.scope("requeue")

        val bucket = TokenBucket.newLeakyBucket(10.seconds, Cost*10*10)  // A reserve of 10/second
        val requeues = sr.counter("requeues")
        val requeueFilter = new RequeueFilter[Req, Rep](bucket, Cost, requeues, next.status)

        def applyNext(conn: ClientConnection, n: Int): Future[Service[Req, Rep]] = {
          next.apply(conn) rescue {
            case f: Failure if f.isFlagged(Failure.Restartable) && !f.isFlagged(Failure.Interrupted) && n > 0 => 
              requeues.incr()
              applyNext(conn, n-1)

            case WriteException(exc) if n > 0 => 
              requeues.incr()
              applyNext(conn, n-1)
          }
        }

        new ServiceFactoryProxy(next) {
          // We define the gauge inside of the ServiceFactory so that their lifetimes
          // are tied together.
          private[this] val budgetGauge = sr.addGauge("budget") { bucket.count/Cost }

          override def apply(conn: ClientConnection): Future[Service[Req, Rep]] =
            applyNext(conn, Effort).map(service => requeueFilter andThen service)
        }
      }
    }
}
