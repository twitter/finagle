package com.twitter.finagle.exp

import com.twitter.util.{Duration, Stopwatch}
import scala.collection.mutable

class ManualStopwatch extends Stopwatch {
  @volatile private[this] var es = mutable.Buffer[Duration]()

  def tick(d: Duration) {
    for (i <- 0 until es.size)
      es(i) += d
  }

  def start() = {
    val i = es.size
    es += Duration.Zero
    () => es(i)
  }
}
