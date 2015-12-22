package com.twitter.finagle.mux.lease

import java.lang.management.GarbageCollectorMXBean
import scala.language.implicitConversions

/**
 * This is the experimental package of mux.lease.  Right now, this is all
 * experimental code around leasing, especially leasing around garbage
 * collections.  We haven't hammered out exactly what the api will be, so it's
 * in large part private and is subject to change.
 *
 * If you want to experiment with GC avoidance, you need to turn on the flag
 * `com.twitter.finagle.mux.lease.exp.drainerEnabled` to enable GC avoidance,
 * and right now it only works with parallel old and CMS.  So far, we've seen
 * promising results with GC avoidance locally, but haven't had the opportunity
 * to run scientific load tests when simulating production load.
 *
 * It can be useful to turn on the
 * `com.twitter.finagle.mux.lease.exp.drainerDebug` flag to get much more
 * granular data about what is going on.  Turning on this flag will log stats
 * for each garbage collection avoidance attempt.  Especially interesting is to
 * see how long it takes for a server to drain, how many outstanding requests
 * there still are, and whether any GCs were natural instead of triggered.  If
 * there are still outstanding requests when the GC is triggered, it might make
 * sense to turn on nacking, or allow more time for draining.
 *
 * You can turn on nacking for a server after the lease has expired by turning
 * on the flag `com.twitter.finagle.mux.lease.exp.nackOnExpiredLease`.  The way
 * this behaves is that it nacks all requests which come in after a 0 lease has
 * been issued. This has two effects--it corrects behavior for clients which are
 * slow to respect leases and turns on failure accrual for clients which
 * continue to send requests.  One possible area of future work would be
 * allowing consumers to configure whether or not Finagle's failure accrual
 * mechanism considers these nacks to be failures.  Another possible improvement
 * would be to nack requests which are already partially completed, instead of
 * just new incoming requets.
 *
 * The `com.twitter.finagle.mux.lease.exp.drainerDiscountRange` flag modulates
 * how the server chooses the discount, which is when it expires the lease.  The
 * left is the absolute lower bound on the range from which it will select
 * randomly.  The right is the absolute upper bound on the range from which it
 * will select randomly.  It chooses a uniform random number of bytes from
 * between the upper bound and the max of the percentile number of bytes
 * generated in a single request and the lower bound.  The absolute lower bound
 * does double duty--it is also used for choosing when to stop waiting for
 * stragglers and run a GC regardless.
 *
 * The `com.twitter.finagle.mux.lease.exp.drainerPercentile` flag specifies the
 * percentile of the incoming request distribution that will be chosen for
 * deciding how long it takes to handle a single request.
 *
 * If you're not working with a JDK which supports System.minorGc, GCA will not
 * work properly, since it will not just run a minor GC when you trigger
 * System.gc.  Although the behavior will continue to be correct, it will be
 * less efficient.  Assuming you're draining properly, your pause times won't
 * affect your latency, but you will have lower throughput, since your server
 * will spend more time not receiving traffice.  One possible direction for
 * further work is to allocate big arrays until you trigger a minor GC, but this
 * must be done slightly delicately to avoid triggering a major collection.
 *
 * The drainerDiscountRange is of insufficient fidelity.  The biggest problem is
 * that it doesn't make sense to conflate the hard cutoff for triggering a GC
 * with the drainerDiscountRange minimum, so it should probably be separated
 * out.
 *
 * Observing leases on the client side can be made slightly smarter too.  It
 * might make sense to have some kind of communication which encourages clients
 * to stop sending requests that it guesses will soon fail (for leases that will
 * soon expire).
 *
 * NB: large parts of this package might suddenly end up in util-jvm
 */
package object exp {
  implicit def gcMxBeanToGc(coll: GarbageCollectorMXBean): GarbageCollectorAddable =
    new GarbageCollectorAddable(coll)
}
