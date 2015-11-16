package com.twitter.finagle.validate

import com.twitter.finagle.load.{Event, LoadGenerator}
import com.twitter.finagle.service.TimeoutFilter
import com.twitter.util.{Duration, Future, MockTimer, Time, Try}
import com.twitter.conversions.time.longToTimeableNumber
import java.io.{BufferedReader, InputStreamReader}
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer

@RunWith(classOf[JUnitRunner])
class ValidateNaiveTimeoutFilter extends FunSuite {
  def getRealData(n: Int): Iterator[Long] = {
    val cl: ClassLoader = getClass().getClassLoader();
    val input: InputStreamReader = new InputStreamReader(
      cl.getResourceAsStream("resources/real_latencies.data"));

    val reader: BufferedReader = new BufferedReader(input);

    reader.mark(n)
    new Iterator[String] {
      def hasNext = true
      def next(): String = {
        Option(reader.readLine()) getOrElse {
          reader.reset()
          reader.readLine()
        }
      }
    } filter (_.nonEmpty) map (opt => (1000 * opt.toDouble).toLong) take (n)
  }

  test("Timeout kills everything over timeout") {
    val now = Time.now
    val timer = new MockTimer
    val total = 10000
    var num = 0
    var success = 0
    var failure = 0
    val data = getRealData(total).toSeq
    val median = data.sorted.apply(total / 2)
    val timeout = median.milliseconds - 1.nanosecond
    val filter = new TimeoutFilter[Event[Boolean, Boolean], Boolean](timeout, timer)
    val gen =
      new LoadGenerator(
        data.map { latency =>
          new Event(now, latency.milliseconds, true, { b: Boolean => Try(true) })
        },
        {
          case (duration: Duration, f: Future[Boolean]) => f onSuccess { _ =>
            assert(duration <= timeout)
          } onFailure { _ =>
            assert(duration > timeout)
          } ensure {
            num += 1
          }
        }: (Duration, Future[Boolean]) => Unit,
        filter,
        timer
      )
    gen.execute()
    assert(num == total)
  }
}
