package com.twitter.finagle.exp.fiber_scheduler.util

import java.io.PrintWriter
import java.io.StringWriter
import java.util.Collections
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.JavaConverters.asScalaSetConverter
import com.twitter.concurrent.ForkingScheduler
import com.twitter.concurrent.NamedPoolThreadFactory
import com.twitter.logging.Logger
import com.twitter.util.Duration
import com.twitter.util.Future
import com.twitter.util.Local
import com.twitter.util.Time
import com.twitter.finagle.exp.fiber_scheduler.Config
import com.twitter.finagle.exp.fiber_scheduler.WorkerThread

/**
 * Tracks pending tasks
 */
abstract class PendingTracking[T](scheduler: ForkingScheduler) {
  import PendingTracking.log

  val name: String
  val samples: Int
  val threshold: Duration

  private[this] case class Pending(task: T) {
    val start = Time.now

    private val thread = Thread.currentThread()
    private def worker = Option(WorkerThread()).map(_.currentWorker)
    private val locals = Local.save()
    private val stack = {
      val sw = new StringWriter()
      val pw = new PrintWriter(sw)
      (new Exception("")).printStackTrace(pw)
      sw.toString().replaceFirst("java.lang.Exception:", "")
    }

    override def toString() = {
      val localsStr =
        locals.getClass.getDeclaredFields
          .filter(_.getName.startsWith("v")).map { field =>
            field.setAccessible(true)
            s"${field.get(locals)}"
          }.mkString(", ")
      val threadStack = thread.getStackTrace.mkString("\n\t")
      s"""$name pending for ${Time.now - start}: 
        |  task: $task
        |  origin:
        |    locals: $localsStr
        |    worker: $worker
        |    thread: $thread (${thread.getState})
        |    initial stack: $stack
        |    current stack:\n\t$threadStack
        """.stripMargin
    }
  }

  private[this] final val pending =
    Collections.newSetFromMap(new ConcurrentHashMap[Pending, java.lang.Boolean])
  private[this] final val count = new AtomicInteger

  Executors
    .newScheduledThreadPool(
      1,
      new NamedPoolThreadFactory(s"fiber/pendingTracking/$name", makeDaemons = true))
    .scheduleAtFixedRate(
      () => refresh(),
      0,
      Config.PendingTracking.interval.inMillis,
      TimeUnit.MILLISECONDS)

  private def refresh() = {
    try {
      val l = pending.asScala.filter(p => (Time.now - p.start) > threshold)
      if (l.nonEmpty) {
        log.info(s"${l.size} $name samples pending:\n${l.mkString("\n")}")
        log.info(s"scheduler state: $scheduler")
      }
    } catch {
      case ex: Throwable =>
        ex.printStackTrace()
        log.error(s"$name pending tracking log error", ex)
    }
  }

  protected def track(v: T): () => Unit = {
    val s = count.get
    if (s < samples && count.compareAndSet(s, s + 1)) {
      val p = Pending(v)
      pending.add(p)
      () => {
        pending.remove(p)
        count.decrementAndGet()
      }
    } else {
      null
    }
  }

}

object PendingTracking {
  private val log = Logger()

  case class ForRunnable(
    scheduler: ForkingScheduler,
    name: String,
    samples: Int,
    threshold: Duration)
      extends PendingTracking[Runnable](scheduler) {

    def apply(r: Runnable): Runnable = {
      val cb = track(r)
      if (cb != null) { () =>
        r.run()
        cb()
      } else {
        r
      }
    }
  }

  case class ForFuture(scheduler: ForkingScheduler, name: String, samples: Int, threshold: Duration)
      extends PendingTracking[Future[_]](scheduler) {

    def apply[T](f: Future[T]): Future[T] = {
      val cb = track(f)
      if (cb != null) {
        f.ensure(cb())
      } else {
        f
      }
    }
  }
}
