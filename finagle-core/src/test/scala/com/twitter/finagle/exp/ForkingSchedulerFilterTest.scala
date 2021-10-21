package com.twitter.finagle.exp

import com.twitter.concurrent.ForkingScheduler
import com.twitter.finagle.Service
import com.twitter.util.{Awaitable, Future}
import java.util.concurrent.{Executor, ExecutorService}
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
    val s = new TestForkingScheduler
    val filter = new ForkingSchedulerFilter.Server[Int, Int](s)
    assert(filter(input, next) == result)
    assert(s.forked == Some(result))
  }

  class TestForkingScheduler extends ForkingScheduler {
    var forked = Option.empty[Future[_]]
    override def fork[T](f: => Future[T]) = {
      val r = f
      forked = Some(r)
      r
    }
    override def fork[T](executor: Executor)(f: => Future[T]) = ???
    override def asExecutorService(): ExecutorService = ???
    override def redirectFuturePools(): Boolean = ???
    override def submit(r: Runnable): Unit = ???
    override def flush(): Unit = ???
    override def numDispatches: Long = ???
    override def blocking[T](f: => T)(implicit perm: Awaitable.CanAwait): T = ???
  }

}
