package com.twitter

import com.twitter.finagle.{Path, Resolver}
import com.twitter.finagle.serverset2.Zk2Resolver
import com.twitter.finagle.serverset2.naming.{IdPrefixingNamer, ServersetNamer}

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
class serverset(zk2: Zk2Resolver)
    extends IdPrefixingNamer(Path.Utf8("$", "com.twitter.serverset"), new ServersetNamer(zk2)) {
  def this() = this(Resolver.get(classOf[Zk2Resolver]).get)
}
