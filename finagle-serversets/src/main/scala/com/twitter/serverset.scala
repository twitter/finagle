package com.twitter

import com.twitter.finagle.{Addr, NameTree, Namer, Resolver, Path, Name}
import com.twitter.util.{Var, Activity}

/**
 * A namer for serverset paths of the form /zk-hosts/path... where zk-hosts is
 * a zk connect string like 'zk.foo.com:2181'.  Naming is performed by way of a
 * Resolver.
 */
private[twitter] trait BaseServersetNamer extends Namer {

  /** Resolve a resolver string to a Var[Addr]. */
  protected[this] def resolve0(spec: String): Var[Addr] = Resolver.eval(spec) match {
    case Name.Bound(addr) => addr
    case _ => Var.value(Addr.Neg)
  }

  protected[this] def resolveServerset(hosts: String, path: String) = 
    resolve0(s"zk2!$hosts!$path")

  protected[this] def resolveServerset(hosts: String, path: String, endpoint: String) = 
    resolve0(s"zk2!$hosts!$path!$endpoint")

  /** Bind a name. */
  protected[this] def bind(path: Path): Option[Name.Bound]

  // We have to involve a serverset roundtrip here to return a tree. We run the
  // risk of invalidating an otherwise valid tree when there is a bad serverset
  // on an Alt branch that would never be taken. A potential solution to this
  // conundrum is to introduce some form of lazy evaluation of name trees.
  def lookup(path: Path): Activity[NameTree[Name]] = bind(path) match {
    case Some(name) =>
      // We have to bind the name ourselves in order to know whether
      // it resolves negatively.
      Activity(name.addr map {
        case Addr.Bound(_, _) => Activity.Ok(NameTree.Leaf(name))
        case Addr.Neg => Activity.Ok(NameTree.Neg)
        case Addr.Pending => Activity.Pending
        case Addr.Failed(exc) => Activity.Failed(exc)
      })

    case None => Activity.value(NameTree.Neg)
  }
}

/**
 * The serverset namer takes [[com.twitter.finagle.Path Paths]] of the form
 *
 * {{{
 * hosts/path...
 * }}}
 *
 * and returns a dynamic represention of the resolution of the path into a
 * tree of [[com.twitter.finagle.Name Names]].
 *
 * The namer synthesizes nodes for each endpoint in the serverset.
 * Endpoint names are delimited by the ':' character. For example
 *
 * {{{
 * /$/com.twitter.serverset/sdzookeeper.local.twitter.com:2181/twitter/service/cuckoo/prod/read:http
 * }}}
 *
 * is the endpoint `http` of serverset `/twitter/service/cuckoo/prod/read` on
 * the ensemble `sdzookeeper.local.twitter.com:2181`.
 */
class serverset extends BaseServersetNamer {
  private[this] val idPrefix = Path.Utf8("$", "com.twitter.serverset")

  protected[this] def bind(path: Path): Option[Name.Bound] = path match {
    case Path.Utf8(hosts, rest@_*) =>
      val addr = if (rest.nonEmpty && (rest.last contains ":")) {
        val Array(name, endpoint) = rest.last.split(":", 2)
        val zkPath = (rest.init :+ name).mkString("/", "/", "")
        resolveServerset(hosts, zkPath, endpoint)
      } else {
        val zkPath = rest.mkString("/", "/", "")
        resolveServerset(hosts, zkPath)
      }

      // Clients may depend on Name.Bound ids being Paths which resolve
      // back to the same Name.Bound
      val id = idPrefix ++ path
      Some(Name.Bound(addr, id))

    case _ => None
  }

}
