package com.twitter.finagle.serverset2.naming

import com.twitter.finagle.{Addr, Path, Name, Namer, NameTree}
import com.twitter.finagle.serverset2.Zk2Resolver
import com.twitter.util.{Activity, Var, Try, Return, Throw}

/**
 * A namer for serverset paths of the form /zk-hosts/path... where
 * zk-hosts is a zk connect string like 'zk.foo.com:2181'.  Naming is
 * performed by way of a Resolver.
 *
 * @param zk2 The underlying serverset resolver
 */
class ServersetNamer(zk2: Zk2Resolver) extends Namer {
  // We have to involve a serverset roundtrip here to return a tree. We run the
  // risk of invalidating an otherwise valid tree when there is a bad serverset
  // on an Alt branch that would never be taken. A potential solution to this
  // conundrum is to introduce some form of lazy evaluation of name trees.
  def lookup(path: Path): Activity[NameTree[Name]] =
    Try(bind(path)) match {
      case Return(va) =>
        // We have to bind the name ourselves in order to know whether
        // it resolves negatively.
        val name = Name.Bound(va, path)
        val act = Activity(va.map(Activity.Ok(_)))
        act.flatMap {
          case Addr.Bound(_, _) => Activity.value(NameTree.Leaf(name))
          case Addr.Neg => Activity.value(NameTree.Neg)
          case Addr.Pending => Activity.pending
          case Addr.Failed(exc) => Activity.exception(exc)
        }
      case Throw(e) => Activity.exception(e)
    }

  private[this] def bind(path: Path): Var[Addr] = ServersetPath.of(path) match {
    case Some(ServersetPath(zkHosts, zkPath, endpoint, shardId)) =>
      zk2.addrOf(zkHosts, zkPath.show, endpoint, shardId)
    case _ =>
      Var.value(Addr.Neg)
  }
}
