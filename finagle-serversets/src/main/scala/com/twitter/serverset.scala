package com.twitter

import com.twitter.app.GlobalFlag
import com.twitter.finagle.serverset2.Zk2Resolver
import com.twitter.finagle.zookeeper.ZkResolver
import com.twitter.finagle.{Addr, Dtab, NameTree, Namer, Resolver, Path, WeightedInetSocketAddress, Name}
import com.twitter.util.Activity
import java.net.InetSocketAddress

object newZk extends GlobalFlag(
  true,
  "If set to true, the new zk2 com.twitter.finagle.Resolver is used. Otherwise, " +
  "an older, less reliable zookeeper client is used."
)

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
class serverset extends Namer {
  private[this] val whichZk = if (newZk()) "zk2" else "zk"

  // We have to involve a serverset roundtrip here to return a tree. We run the
  // risk of invalidating an otherwise valid tree when there is a bad serverset
  // on an Alt branch that would never be taken. A potential solution to this
  // conundrum is to introduce some form of lazy evaluation of name trees.
  def lookup(path: Path): Activity[NameTree[Name]] = path match {
    case Path.Utf8(hosts, rest@_*) =>
      val spec = if (rest.nonEmpty && (rest.last contains ":")) {
        val Array(name, endpoint) = rest.last.split(":", 2)
        val path = rest.init :+ name
        "%s!%s!/%s!%s".format(whichZk, hosts,  path mkString "/", endpoint)
      } else {
        "%s!%s!/%s".format(whichZk, hosts, rest mkString "/")
      }

      val Name.Bound(va) = Resolver.eval(spec)
      // Clients may depend on Name.Bound ids being Paths which resolve
      // back to the same Name.Bound
      val id = Path.Utf8("$", "com.twitter.serverset") ++ path
      val name = Name.Bound(va, id)

      // We have to bind the name ourselves in order to know whether
      // it resolves negatively.
      Activity(va map {
        case Addr.Bound(_) => Activity.Ok(NameTree.Leaf(name))
        case Addr.Neg => Activity.Ok(NameTree.Neg)
        case Addr.Pending => Activity.Pending
        case Addr.Failed(exc) => Activity.Failed(exc)
      })


    case _ =>
      Activity.exception(new Exception("Invalid com.twitter.namer path "+path.show))
  }

  def enum(prefix: Path): Activity[Dtab] = Activity.value(Dtab.empty)
}
