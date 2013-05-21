package com.twitter.finagle.loadbalancer

import com.twitter.finagle.{ClientConnection, Group, Service, ServiceFactory}
import com.twitter.util.{Await, Future, Stopwatch, Time}

object Benchmark {
  // todo: simulate distributions of loads.
  private[this] val N = 1<<19
  private[this] val W = 1 //1000
  private[this] val F = 500

  private[this] val loads = new Array[Int](F)

  private[this] val service = new Service[Int, Int] {
    def apply(req: Int) = null
  }
  private[this] class LoadedServiceFactory(i: Int) extends ServiceFactory[Int, Int] {
    def apply(conn: ClientConnection) = { loads(i) += 1; Future.value(service) }
    def close(deadline: Time) = Future.Done
  }
  private[this] val factories = 0 until F map { i => new LoadedServiceFactory(i) }

  def reset() {
    0 until loads.size foreach { i => loads(i) = 0 }
  }

  def bench(factory: ServiceFactory[_, _]) = {
    val elapsed = Stopwatch.start()
    val outstanding = new Array[Service[_, _]](W)
    0 until N foreach { i =>
      val j = i % W
      // todo: jitter in release. pick one at random.
      if (outstanding(j) ne null) outstanding(j).close()
      outstanding(j) = Await.result(factory())
    }
    elapsed()
  }

  def go(factory: ServiceFactory[_, _], name: String) = {
    printf("warming up %s..\n", name)
    bench(factory)
    reset()
    printf("measuring %s..\n", name)
    val elapsed = bench(factory)
    printf("%dms - %d ops/sec\n",
      elapsed.inMilliseconds, 1000 * N/elapsed.inMilliseconds)
    val histo = loads.sorted.foldLeft(Nil: List[(Int, Int)]) {
      case ((load, count) :: rest, thisLoad) if load == thisLoad =>
        (load, count + 1) :: rest
      case (rest, thisLoad) =>
        (thisLoad, 1) :: rest
    }

    printf("loads: %s\n", histo mkString " ")
  }

  def main(args: Array[String]) {
    val group = Group[ServiceFactory[Int, Int]](factories:_*)
    val heap = new HeapBalancer(group)

    go(heap, "Heap")
  }
}
