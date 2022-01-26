package com.twitter.finagle.exp

import com.twitter.concurrent.ForkingScheduler
import com.twitter.conversions.DurationOps._
import com.twitter.finagle.Service
import com.twitter.util.Awaitable
import com.twitter.util.Future
import com.twitter.util.Await
import java.util.concurrent.Executor
import java.util.concurrent.ExecutorService
import org.scalatest.funsuite.AnyFunSuite

class ForkingSchedulerFilterTest extends AnyFunSuite {

  test("forks the execution using the forking scheduler") {
    val input = 42
    val output = 43
    val result = Future.value(output)
    val next = Service.mk[Int, Int] { i =>
      assert(i == input)
      result
    }
    val s = new TestForkingScheduler(fail = false)
    val filter = new ForkingSchedulerFilter.Server[Int, Int](s)
    assert(Await.result(filter(input, next), 1.second) == output)
    assert(s.forked == Some(result))
  }

  test("returns rejection failure if the scheduler is overloaded") {
    val input = 42
    val next = Service.mk[Int, Int](_ => ???)
    val s = new TestForkingScheduler(fail = true)
    val filter = new ForkingSchedulerFilter.Server[Int, Int](s)
    val fut = filter(input, next).liftToTry
    assert(Await.result(fut, 1.second).isThrow)
    assert(s.forked.isEmpty)
  }

  class TestForkingScheduler(fail: Boolean) extends ForkingScheduler {
    var forked = Option.empty[Future[_]]
    override def tryFork[T](f: => Future[T]) = {
      if (!fail) {
        val r = f
        forked = Some(r)
        r.map(Some(_))
      } else {
        Future.None
      }
    }
    override def fork[T](f: => Future[T]) = ???
    override def fork[T](executor: Executor)(f: => Future[T]) = ???
    override def withMaxSyncConcurrency(concurrency: Int, maxWaiters: Int) = ???
    override def asExecutorService(): ExecutorService = ???
    override def redirectFuturePools(): Boolean = ???
    override def submit(r: Runnable): Unit = ???
    override def flush(): Unit = ???
    override def numDispatches: Long = ???
    override def blocking[T](f: => T)(implicit perm: Awaitable.CanAwait): T = ???
  }

}
