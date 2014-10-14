package com.twitter.finagle.serverset2.client.apache

import com.twitter.finagle.serverset2.client.Data

private[serverset2] object ApacheData {
  object ACL {
    def apply(a: org.apache.zookeeper.data.ACL): Data.ACL =
      Data.ACL(a.getPerms, Id(a.getId))

    def zk(a: Data.ACL): org.apache.zookeeper.data.ACL =
      new org.apache.zookeeper.data.ACL(a.perms, Id.zk(a.id))
  }

  object Id {
    def apply(id: org.apache.zookeeper.data.Id): Data.Id =
      Data.Id(id.getScheme, id.getId)

    def zk(id: Data.Id): org.apache.zookeeper.data.Id =
      new org.apache.zookeeper.data.Id(id.scheme, id.id)
  }

  object Stat {
    def apply(st: org.apache.zookeeper.data.Stat): Data.Stat =
      Data.Stat(
        czxid = st.getCzxid,
        mzxid = st.getMzxid,
        ctime = st.getCtime,
        mtime = st.getMtime,
        version = st.getVersion,
        cversion = st.getCversion,
        aversion = st.getAversion,
        ephemeralOwner = st.getEphemeralOwner,
        dataLength = st.getDataLength,
        numChildren = st.getNumChildren,
        pzxid = st.getPzxid)

    def zk(st: Data.Stat): org.apache.zookeeper.data.Stat =
      new org.apache.zookeeper.data.Stat(
        st.czxid,
        st.mzxid,
        st.ctime,
        st.mtime,
        st.version,
        st.cversion,
        st.aversion,
        st.ephemeralOwner,
        st.dataLength,
        st.numChildren,
        st.pzxid)
  }
}
