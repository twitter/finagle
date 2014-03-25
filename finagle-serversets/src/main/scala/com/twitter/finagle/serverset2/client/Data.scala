package com.twitter.finagle.serverset2.client

private[serverset2] object Data {
  object ACL {
    /**
     * This ACL gives the world the ability to read.
     */
    val AnyoneReadUnsafe = ACL(Perms.Read, Id.AnyoneUnsafe)

    /**
     * This is a completely open ACL .
     */
    val AnyoneAllUnsafe = ACL(Perms.All, Id.AnyoneUnsafe)

    /**
     * This ACL gives the creators authentication id's all permissions.
     */
    val CreatorAll = ACL(Perms.All, Id.AuthIds)
  }

  case class ACL(perms: Int, id: Id)

  object Id {
    /**
     * This Id represents anyone.
     */
    val AnyoneUnsafe = Id("world", "anyone")

    /**
     * This Id is only usable to set ACLs. It will get substituted with the
     * Id's the client authenticated with.
     */
    val AuthIds = Id("auth", "")
  }

  case class Id(scheme:String, id: String)

  case class Stat(
      czxid: Long, // ZXID that created this node
      mzxid: Long, // ZXID that last modified this node
      ctime: Long, // Creation time
      mtime: Long, // Modification time
      version: Int, // Znode version
      cversion: Int, // Child version
      aversion: Int, // ACL version
      ephemeralOwner: Long, // Owner ID if ephemeral, 0 otherwise
      dataLength: Int, // Length of the data field in bytes
      numChildren: Int, // Number of children of this node
      pzxid: Long) // ZXID that last modified children of this node
}
