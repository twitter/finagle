package com.twitter.finagle.serverset2

import com.twitter.finagle.Addr
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.finagle.util.DefaultTimer
import com.twitter.logging.Logger
import com.twitter.util.Await
import com.twitter.util.Duration
import com.twitter.util.Promise
import org.openjdk.jmh.annotations._
import java.util.concurrent.TimeUnit

/**
 * Investigate serverset resolver allocations when members are thrashing in zookeeper. Intended to
 * be run before and after making a change to detect memory leaks or large differences in allocation
 * patterns.
 *
 * 1. Run the server separately to isolate client allocations:
 *  ./bazel run finagle/finagle-benchmark/src/main/scala:serverset-service
 *
 * 2. Run this client in sbt
 * $ ./sbt
 * > project finagle-benchmark
 * > run ServerSetResolver -prof gc
 */
@State(Scope.Benchmark)
@Fork(1)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 1)
@BenchmarkMode(Array(Mode.SingleShotTime))
class ServerSetResolver {

  // These params should be kept in sync with the corresponding flags in
  // LocalServerSetService.
  @Param(Array("2181")) var zkListenPort = 2181
  @Param(Array("25")) var serverSetsToResolve = 25

  @Param(Array("1")) var stabilizationSec = 1

  /**
   * Specifies how long the test runs (as serverset monitoring
   * will continue for the lifetime of the test).
   */
  @Param(Array("120")) var testRuntimeSec = 120

  val logger = Logger(getClass)

  /**
   * This test will use the zk2 resolver to resolve and monitor updates to N serversets.
   * The tests counterpart [[LocalServerSetService]] will be (deterministically) thrashing
   * the serverset by constantly adding and removing members.
   *
   * This should be used for measuring allocation and GC impact of serverset resolution
   * during zk churn.
   */
  @Benchmark
  def resolveChurningServerSets(): Int = {

    val shutdown = new Promise[Int]
    implicit val timer = DefaultTimer

    val stabilizationWindow = Duration.fromSeconds(stabilizationSec)
    val resolver = new Zk2Resolver(
      NullStatsReceiver
    )

    val serverSetPaths = LocalServerSetService.createServerSetPaths(serverSetsToResolve)

    // For the lifetime of this test, monitor changes to all N serversets
    // (The resolver is always monitoring changes)
    serverSetPaths.foreach { path => monitorServersetChanges(resolver, path) }

    // The resolver is always updating as its zk-backed serverset is changing.
    // Run this test for `testRuntimeSec` seconds to see the impact over
    // an extended time.
    timer.doLater(Duration.fromSeconds(testRuntimeSec)) { shutdown.setValue(1) }
    Await.result(shutdown)
  }

  /**
   * Resolve and monitor changes to a single serverset for the lifetime of the test.
   */
  def monitorServersetChanges(resolver: Zk2Resolver, zkPath: String): Unit = {
    resolver.bind(s"localhost:$zkListenPort!$zkPath").changes.respond {
      case Addr.Bound(set, metadata) => logger.info(s"Serverset $zkPath has ${set.size} entries")
      case Addr.Neg => unexpectedError(s"negative resolution of $zkPath")
      case Addr.Failed(exc) => unexpectedError(s"$zkPath: Addr.Failure[$exc]")
      case Addr.Pending => logger.info(s"$zkPath is pending...")
    }
  }

  def unexpectedError(msg: String) =
    throw new IllegalStateException(s"Unexpected resolution failure. $msg")
}
