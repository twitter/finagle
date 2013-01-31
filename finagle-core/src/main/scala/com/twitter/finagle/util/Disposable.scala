package com.twitter.finagle.util

import com.twitter.util.{Future, Return, Throw, Time}

/**
 * Disposable is a container for a resource that must be explicitly disposed when
 * no longer needed. After this, the resource is no longer available.
 */
trait Disposable[+T] {
  def get: T
  /**
   * Dispose of a resource by deadline given.
   */
  def dispose(deadline: Time): Future[Unit]
  final def dispose(): Future[Unit] = dispose(Time.Top)
}

object Disposable {
  def const[T](t: T) = new Disposable[T] {
    def get = t
    def dispose(deadline: Time) = Future.value(())
  }
}


/**
 * `Managed[T]` is a resource of type `T` which lifetime is explicitly managed.
 * It is created with `make()`. Composite resources, which lifetimes are managed
 * together, are created by the use of `flatMap`, ensuring proper construction and
 * teardown of the comprised resources.
 */
trait Managed[+T] { selfT =>

  /**
   * Create a new T, and pass it to the given operation (f).
   * After it completes, the resource is disposed.
   */
  def foreach(f: T => Unit) {
    val r = this.make()
    try f(r.get) finally r.dispose()
  }

  /**
   * Compose a new managed resource that depends on `this' managed resource.
   */
  def flatMap[U](f: T => Managed[U]): Managed[U] = new Managed[U] {
    def make() = new Disposable[U] {
      val t = selfT.make()

      val u = try {
        f(t.get).make()
      } catch {
        case e: Exception =>
          t.dispose()
          throw e
      }

      def get = u.get

      def dispose(deadline: Time) = {
        u.dispose(deadline) transform {
          case Return(_) => t.dispose(deadline)
          case Throw(outer) => t.dispose transform {
            case Throw(inner) => Future.exception(new DoubleTrouble(outer, inner))
            case Return(_) => Future.exception(outer)
          }
        }
      }
    }
  }

  def map[U](f: T => U): Managed[U] = flatMap { t => Managed.const(f(t)) }

  /**
   * Builds a resource.
   */
  def make(): Disposable[T]
}

object Managed {
  def singleton[T](t: Disposable[T]) = new Managed[T] { def make() = t }
  def const[T](t: T) = singleton(Disposable.const(t))
}

class DoubleTrouble(cause1: Throwable, cause2: Throwable) extends Exception {
  override def getStackTrace = cause1.getStackTrace
  override def getMessage =
    "Double failure while disposing composite resource: %s \n %s".format(
      cause1.getMessage, cause2.getMessage)
}
