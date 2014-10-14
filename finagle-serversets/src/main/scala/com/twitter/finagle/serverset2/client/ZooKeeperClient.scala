package com.twitter.finagle.serverset2.client

import com.twitter.io.Buf
import com.twitter.util.{Closable, Duration, Future}

private[serverset2] trait ZooKeeperClient extends Closable {
  /**
   * The session id for this ZooKeeper client instance. The value returned is
   * not valid until the client connects to a server and may change after a
   * re-connect.
   *
   * @return current session id
   */
  def sessionId: Long

  /**
   * The session password for this ZooKeeper client instance. The value
   * returned is not valid until the client connects to a server and may
   * change after a re-connect.
   *
   * @return current session password
   */
  def sessionPasswd: Buf

  /**
   * The negotiated session timeout for this ZooKeeper client instance. The
   * value returned is not valid until the client connects to a server and
   * may change after a re-connect.
   *
   * @return current session timeout
   */
  def sessionTimeout: Duration

  /**
   * Add the specified scheme: auth information to this connection.
   *
   * @param scheme the authentication scheme to use.
   * @param auth the authentication credentials.
   * @return a Future[Unit]
   */
   def addAuthInfo(scheme: String, auth: Buf): Future[Unit]

  /**
   * Get the existing ephemeral nodes created with the current session ID.
   *
   * NOTE: This method is not universally implemented. The Future will fail
   * with KeeperException.Unimplemented if this is the case.
   *
   * @return a Future[Seq[String]] of ephemeral node paths.
   */

  def getEphemerals(): Future[Seq[String]]

  /**
   * String representation of this ZooKeeper client. Suitable for things
   * like logging.
   *
   * Do NOT count on the format of this string, it may change without
   * warning.
   *
   * @return string representation of the current client
   */
  def toString: String
}

private[serverset2] trait ZooKeeperReader extends ZooKeeperClient {
  /**
   * Check if a node exists.
   *
   * @param path the path of the node to check.
   * @return a Future[Option[Data.Stat] containing Some[Stat] if the node exists,
   *         or None if the node does not exist.
   */
  def exists(path: String): Future[Option[Data.Stat]]

  /**
   * A version of exists that sets a watch and returns a Future[Watched[Option[Data.Stat]]]
   */
  def existsWatch(path: String): Future[Watched[Option[Data.Stat]]]

  /**
   * Return the data of the node of the given path.
   *
   * @param path the path of the node to read.
   * @return a Future[Node.Data]
   */
  def getData(path: String): Future[Node.Data]

  /**
   * A version of getData that sets a watch and returns a Future[Watched[Node.Data]]
   */
  def getDataWatch(path: String): Future[Watched[Node.Data]]

  /**
   * Get the ACL of the node of the given path.
   *
   * @param path the path of the node to read.
   * @return a Future[Node.ACL]
   */
  def getACL(path: String): Future[Node.ACL]

  /**
   * For a node at a given path return its stat and a list of children.
   *
   * @param path the path of the node to read.
   * @return a Future[Node.Children]
   */
  def getChildren(path: String): Future[Node.Children]

  /**
   * A version of getChildren that sets and returns a Future[Watched[Node.Children]]
   */
  def getChildrenWatch(path: String): Future[Watched[Node.Children]]

  /**
   * Sync. Flushes channel between process and leader.
   *
   * @param path the path of the node to sync.
   * @return a Future[Unit]
   */
  def sync(path: String): Future[Unit]
}

object ZooKeeperReader {
  def patToPathAndPrefix(pat: String): (String, String) = {
    if (pat.isEmpty || pat(0) != '/')
      throw new IllegalArgumentException("Invalid glob pattern")

    val slash = pat.lastIndexOf('/')
    if (slash < 0)
      throw new IllegalArgumentException("Invalid prefix")

    val path = if (slash == 0) "/" else pat.substring(0, slash)
    val prefix = pat.substring(slash+1, pat.length)

    (path, prefix)
  }

  /** An implementation helper for ZooKeeperReader.glob */
  def processGlob(path: String, prefix: String, children: java.util.List[String]): Seq[String] = {
    val seq = Seq.newBuilder[String]
    val iter = children.iterator()
    while (iter.hasNext()) {
      val el = iter.next()
      if (el startsWith prefix)
        seq += path+"/"+el
    }
    seq.result
  }
}

private[serverset2] trait ZooKeeperWriter extends ZooKeeperClient {
  /**
   * Create a node of a given type with the given path. The node data will be the
   * given data, and node acl will be the given acl.
   *
   * @param path the path for the node.
   * @param data the initial data for the node.
   * @param acl a sequence of ACLs for the node.
   * @param createMode specifies what type of node to create.
   * @return a Future[String] containing the actual path of the created node.
   */
  def create(
      path: String,
      data: Option[Buf],
      acl: Seq[Data.ACL],
      createMode: CreateMode): Future[String]

  /**
   * Delete the node with the given path. The call will succeed if such a node
   * exists, and the given version matches the node's version (if the given
   * version is None, it matches any node's versions).
   *
   * This operation, if successful, will trigger all the watches on the node
   * of the given path left by existsWatch API calls, and the watches on the parent
   * node left by getChildrenWatch API calls.
   *
   * @param path the path of the node to be deleted.
   * @param version the expected node version.
   * @return a Future[Unit]
   */
  def delete(path: String, version: Option[Int]): Future[Unit]

  /**
   * Set the data for the node of the given path if such a node exists and the
   * given version matches the version of the node (if the given version is None,
   * it matches any node's versions).
   *
   * This operation, if successful, will trigger all the watches on the node
   * of the given path left by getDataWatch calls.
   *
   * @param path the path of the node to write.
   * @param data the data to set.
   * @param version the expected matching version.
   * @return a Future[Data.Stat]
   */
  def setData(path: String, data: Option[Buf], version: Option[Int]): Future[Data.Stat]

  /**
   * Set the ACL for the node of the given path if such a node exists and the
   * given version matches the version of the node (if the given version is None,
   * it matches any node's versions)
   *
   * @param path the path of the node to write.
   * @param acl a list of Data.ACL to apply to the node.
   * @param version the expected matching version.
   * @return a Future[Data.Stat]
   */
  def setACL(path: String, acl: Seq[Data.ACL], version: Option[Int]): Future[Data.Stat]
}

private[serverset2] trait ZooKeeperMulti extends ZooKeeperClient {
  /**
   * Transactional operation. Execute all operations or none of them.
   *
   * @param ops a list of operations to apply.
   * @return a Future[Seq[OpResult]]
   */
  def multi(ops: Seq[Op]): Future[Seq[OpResult]]
}

private[serverset2] trait ZooKeeperRW extends ZooKeeperReader with ZooKeeperWriter

private[serverset2] trait ZooKeeperRWMulti extends ZooKeeperRW with ZooKeeperMulti
