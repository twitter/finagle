package com.twitter

import com.twitter.finagle.{Addr, NameTree, Namer, Resolver, Path, Name}
import com.twitter.finagle.serverset2.naming.ServersetPath
import com.twitter.util.{Var, Activity, Try, Throw, Return}

/**
 * A namer for serverset paths of the form /zk-hosts/path... where zk-hosts is
 * a zk connect string like 'zk.foo.com:2181'.  Naming is performed by way of a
 * Resolver.
 */
private[twitter] trait BaseServersetNamer extends Namer {
  protected[this] val resolverScheme: String = "zk2"

  /** Resolve a resolver string to a Var[Addr]. */
  protected[this] def resolve0(spec: String): Var[Addr] = Resolver.eval(spec) match {
    case Name.Bound(addr) => addr
    case _ => Var.value(Addr.Neg)
  }

  protected[this] def resolveServerset(
    hosts: String,
    path: String,
    endpoint: Option[String],
    shardId: Option[Int]
  ): Var[Addr] = {
    val spec = endpoint match {
      case Some(ep) => s"$resolverScheme!$hosts!$path!$ep"
      case None => s"$resolverScheme!$hosts!$path"
    }
    resolve0(spec)
  }

  /** Bind a name. */
  protected[this] def bind(path: Path): Option[Name.Bound]

  // We have to involve a serverset roundtrip here to return a tree. We run the
  // risk of invalidating an otherwise valid tree when there is a bad serverset
  // on an Alt branch that would never be taken. A potential solution to this
  // conundrum is to introduce some form of lazy evaluation of name trees.
  def lookup(path: Path): Activity[NameTree[Name]] = Try(bind(path)) match {
    case Return(Some(name)) =>
      // We have to bind the name ourselves in order to know whether
      // it resolves negatively.
      Activity(name.addr map {
        case Addr.Bound(_, _) => Activity.Ok(NameTree.Leaf(name))
        case Addr.Neg => Activity.Ok(NameTree.Neg)
        case Addr.Pending => Activity.Pending
        case Addr.Failed(exc) => Activity.Failed(exc)
      })

    case Return(None) => Activity.value(NameTree.Neg)
    case Throw(e) => Activity.exception(e)
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
 * Endpoint names are delimited by the ':' character and shard IDs are delimited by the '#' character.
 * For example:
 *
 * {{{
 * /$/com.twitter.serverset/sdzookeeper.local.twitter.com:2181/twitter/service/cuckoo/prod/read:http#0
 * }}}
 *
 * is the endpoint `http` of shard 0 of serverset `/twitter/service/cuckoo/prod/read` on
 * the ensemble `sdzookeeper.local.twitter.com:2181`.
 */
class serverset extends BaseServersetNamer {
  private[this] val idPrefix = Path.Utf8("$", "com.twitter.serverset")

  protected[this] def bind(path: Path): Option[Name.Bound] = ServersetPath.of(path).map {
    case ServersetPath(zkHosts, zkPath, endpoint, shardId) =>
      val addr = resolveServerset(zkHosts, zkPath.show, endpoint, shardId)
      // Clients may depend on Name.Bound ids being Paths which resolve
      // back to the same Name.Bound
      val id = idPrefix ++ path
      Name.Bound(addr, id)
  }
}
