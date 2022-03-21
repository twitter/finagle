package com.twitter.finagle

import com.twitter.finagle.util.CachedHashCode
import com.twitter.finagle.util.Showable
import com.twitter.util.Var
import scala.annotation.varargs

/**
 * Names identify network locations. They come in two varieties:
 *
 *  1. [[com.twitter.finagle.Name.Bound Bound]] names are concrete.
 *  They represent a changeable list of network endpoints
 *  (represented by [[com.twitter.finagle.Addr Addr]]s).
 *
 *  2. [[com.twitter.finagle.Name.Path Path]] names are unbound
 *  paths, representing an abstract location which must be resolved
 *  by some context, usually the [[com.twitter.finagle.Dtab Dtab]].
 *
 * In practice, clients use a [[com.twitter.finagle.Resolver]] to resolve a
 * destination name string into a `Name`. This is achieved by passing a
 * destination name into methods such as
 * [[com.twitter.finagle.builder.ClientBuilder ClientBuilder.dest]] or
 * the `newClient` method of the appropriate protocol object
 * (e.g. `Http.newClient(/s/org/servicename)`). These APIs use `Resolver` under
 * the hood to resolve the destination names into the `Name` representation
 * of the appropriate cluster.
 *
 * As names are bound, a [[com.twitter.finagle.Namer Namer]] may elect
 * to bind only a [[com.twitter.finagle.Name Name]] prefix, leaving an
 * unbound residual name to be processed by a downstream Namer.
 *
 * @see The [[https://twitter.github.io/finagle/guide/Names.html user guide]]
 *      for further details.
 */
sealed trait Name

/**
 * See [[Names]] for Java compatibility APIs.
 */
object Name {

  /**
   * Path names comprise a [[com.twitter.finagle.Path Path]] denoting a
   * network location.
   */
  case class Path(path: com.twitter.finagle.Path) extends Name with CachedHashCode.ForCaseClass

  /**
   * Bound names comprise a changeable [[Addr]] which carries a host
   * list of internet addresses.
   *
   * Equality of two Names is delegated to `id`. Two Bound instances
   * are equal whenever their `id`s are. `id` identifies the `addr`
   * and not the `path`.  If the `id` is a [[Name.Path]],
   * it should only contain *bound*--not residual--path components.
   *
   * The `path` contains unbound residual path components that were not
   * processed during name resolution.
   */
  class Bound private (val addr: Var[Addr], val id: Any, val path: com.twitter.finagle.Path)
      extends Name
      with Proxy
      with CachedHashCode.ForClass {
    def self: Any = id

    override protected def computeHashCode: Int = self.hashCode

    def idStr: String =
      id match {
        case path: com.twitter.finagle.Path => path.show
        case _ => id.toString
      }
  }

  object Bound {
    def apply(addr: Var[Addr], id: Any, path: com.twitter.finagle.Path): Name.Bound =
      new Bound(addr, id, path)

    def apply(addr: Var[Addr], id: Any): Name.Bound =
      apply(addr, id, com.twitter.finagle.Path.empty)

    def unapply(name: Name.Bound): Option[Var[Addr]] = Some(name.addr)

    /**
     * Create a singleton address, equal only to itself.
     */
    def singleton(addr: Var[Addr]): Name.Bound = Name.Bound(addr, new Object())
  }

  // So that we can print NameTree[Name]
  implicit val showable: Showable[Name] = new Showable[Name] {
    def show(name: Name): String = name match {
      case Path(path) => path.show
      case bound @ Bound(_) =>
        bound.id match {
          case id: com.twitter.finagle.Path => id.show
          case id => id.toString
        }
    }
  }

  /**
   * Create a pre-bound address.
   * @see [[Names.bound]] for Java compatibility.
   */
  def bound(addrs: Address*): Name.Bound =
    Name.Bound(Var.value(Addr.Bound(addrs: _*)), addrs.toSet)

  /**
   * Create a pre-bound [[Address]] which points directly to the provided [[Service]].
   *
   * @note This method can be extremely useful in testing the functionality of a Finagle
   * client without involving the network.
   *
   * {{{
   *   import com.twitter.conversions.DurationOps._
   *   import com.twitter.finagle.{Http, Name, Service}
   *   import com.twitter.finagle.http.{Request, Response, Status}
   *   import com.twitter.util.{Await, Future}
   *
   *   val service: Service[Request, Response] = Service.mk { request =>
   *     val response = Response()
   *     response.status = Status.Ok
   *     response.contentString = "Hello"
   *     Future.value(response)
   *   }
   *   val name = Name.bound(service)
   *   val client = Http.client.newService(name, "hello-service-example")
   *   val result = Await.result(client(Request("/")), 1.second)
   *   result.contentString // "Hello"
   * }}}
   */
  def bound[Req, Rep](service: Service[Req, Rep]): Name.Bound =
    bound(Address.ServiceFactory[Req, Rep](ServiceFactory.const(service)))

  /**
   * An always-empty name.
   * @see [[Names.empty]] for Java compatibility.
   */
  val empty: Name.Bound = bound()

  /**
   * Create a path-based Name which is interpreted vis-à-vis
   * the current request-local delegation table.
   * @see [[Names.fromPath]] for Java compatibility.
   */
  def apply(path: com.twitter.finagle.Path): Name =
    Name.Path(path)

  /**
   * Create a path-based Name which is interpreted vis-à-vis
   * the current request-local delegation table.
   * @see [[Names.fromPath]] for Java compatibility.
   */
  def apply(path: String): Name =
    Name.Path(com.twitter.finagle.Path.read(path))

  // Create a name representing the union of the passed-in names.
  // Metadata is not preserved on bound addresses.
  private[finagle] def all(names: Set[Name.Bound]): Name.Bound =
    if (names.isEmpty) empty
    else if (names.size == 1) names.head
    else {
      val va = Var.collect(names.view.map(_.addr).toSeq) map {
        case addrs if addrs.exists({ case Addr.Bound(_, _) => true; case _ => false }) =>
          val endpointAddrs = addrs.view.flatMap {
            case Addr.Bound(as, _) => as
            case _ => Seq.empty[Address]
          }
          Addr.Bound(endpointAddrs.toSet, Addr.Metadata.empty)

        case addrs if addrs.forall(_ == Addr.Neg) => Addr.Neg
        case addrs if addrs.forall({ case Addr.Failed(_) => true; case _ => false }) =>
          Addr.Failed(new Exception)

        case _ => Addr.Pending
      }

      val id = names map { case bound @ Name.Bound(_) => bound.id }
      Name.Bound(va, id)
    }
}

/**
 * Java compatibility APIs for [[Name]].
 */
object Names {

  /** See [[Name.bound]] */
  @varargs
  def bound(addrs: Address*): Name.Bound =
    Name.bound(addrs: _*)

  /** See [[Name.bound]] */
  def bound[Req, Rep](service: Service[Req, Rep]): Name.Bound =
    Name.bound(service)

  /** See [[Name.empty]] */
  def empty: Name.Bound =
    Name.empty

  /** See [[Name.apply]] */
  def fromPath(path: com.twitter.finagle.Path): Name =
    Name(path)

  /** See [[Name.apply]] */
  def fromPath(path: String): Name =
    Name(path)

}
