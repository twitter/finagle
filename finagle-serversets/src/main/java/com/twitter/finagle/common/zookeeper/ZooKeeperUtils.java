// =================================================================================================
// Copyright 2011 Twitter, Inc.
// -------------------------------------------------------------------------------------------------
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this work except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file, or at:
//
//  https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// =================================================================================================

package com.twitter.finagle.common.zookeeper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.logging.Logger;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.common.PathUtils;
import org.apache.zookeeper.data.ACL;

import com.twitter.finagle.common.zookeeper.ZooKeeperClient.ZooKeeperConnectionException;
import com.twitter.util.Duration;

/**
 * Utilities for dealing with zoo keeper.
 */
public final class ZooKeeperUtils {

  private static final Logger LOG = Logger.getLogger(ZooKeeperUtils.class.getName());

  /**
   * An appropriate default session timeout for Twitter ZooKeeper clusters.
   */
  public static final Duration DEFAULT_ZK_SESSION_TIMEOUT = Duration.fromSeconds(4);

  /**
   * The magic version number that allows any mutation to always succeed regardless of actual
   * version number.
   */
  public static final int ANY_VERSION = -1;

  /**
   * An ACL that gives all permissions any user authenticated or not.
   */
  public static final List<ACL> OPEN_ACL_UNSAFE =
      Collections.unmodifiableList(new ArrayList<>(Ids.OPEN_ACL_UNSAFE));

  /**
   * An ACL that gives all permissions to node creators and read permissions only to everyone else.
   */
  public static final List<ACL> EVERYONE_READ_CREATOR_ALL;
  static {
    List<ACL> list = new ArrayList<>(Ids.CREATOR_ALL_ACL.size() + Ids.READ_ACL_UNSAFE.size());
    list.addAll(Ids.CREATOR_ALL_ACL);
    list.addAll(Ids.READ_ACL_UNSAFE);
    EVERYONE_READ_CREATOR_ALL = Collections.unmodifiableList(list);
  }

  /**
   * Returns true if the given exception indicates an error that can be resolved by retrying the
   * operation without modification.
   *
   * @param e the exception to check
   * @return true if the causing operation is strictly retryable
   */
  public static boolean isRetryable(KeeperException e) {
    Objects.requireNonNull(e);

    switch (e.code()) {
      case CONNECTIONLOSS:
      case SESSIONEXPIRED:
      case SESSIONMOVED:
      case OPERATIONTIMEOUT:
        return true;

      case RUNTIMEINCONSISTENCY:
      case DATAINCONSISTENCY:
      case MARSHALLINGERROR:
      case BADARGUMENTS:
      case NONODE:
      case NOAUTH:
      case BADVERSION:
      case NOCHILDRENFOREPHEMERALS:
      case NODEEXISTS:
      case NOTEMPTY:
      case INVALIDCALLBACK:
      case INVALIDACL:
      case AUTHFAILED:
      case UNIMPLEMENTED:

      // These two should not be encountered - they are used internally by ZK to specify ranges
      case SYSTEMERROR:
      case APIERROR:

      case OK: // This is actually an invalid ZK exception code

      default:
        return false;
    }
  }

  /**
   * Ensures the given {@code path} exists in the ZK cluster accessed by {@code zkClient}.  If the
   * path already exists, nothing is done; however if any portion of the path is missing, it will be
   * created with the given {@code acl} as a persistent zookeeper node.  The given {@code path} must
   * be a valid zookeeper absolute path.
   *
   * @param zkClient the client to use to access the ZK cluster
   * @param acl the acl to use if creating path nodes
   * @param path the path to ensure exists
   * @throws ZooKeeperConnectionException if there was a problem accessing the ZK cluster
   * @throws InterruptedException if we were interrupted attempting to connect to the ZK cluster
   * @throws KeeperException if there was a problem in ZK
   */
  public static void ensurePath(ZooKeeperClient zkClient, List<ACL> acl, String path)
      throws ZooKeeperConnectionException, InterruptedException, KeeperException {
    Objects.requireNonNull(zkClient);
    Objects.requireNonNull(path);
    if (!path.startsWith("/")) {
      throw new IllegalArgumentException();
    }

    ensurePathInternal(zkClient, acl, path);
  }

  private static void ensurePathInternal(ZooKeeperClient zkClient, List<ACL> acl, String path)
      throws ZooKeeperConnectionException, InterruptedException, KeeperException {
    if (zkClient.get().exists(path, false) == null) {
      // The current path does not exist; so back up a level and ensure the parent path exists
      // unless we're already a root-level path.
      int lastPathIndex = path.lastIndexOf('/');
      if (lastPathIndex > 0) {
        ensurePathInternal(zkClient, acl, path.substring(0, lastPathIndex));
      }

      // We've ensured our parent path (if any) exists so we can proceed to create our path.
      try {
        zkClient.get().create(path, null, acl, CreateMode.PERSISTENT);
      } catch (KeeperException.NodeExistsException e) {
        // This ensures we don't die if a race condition was met between checking existence and
        // trying to create the node.
        LOG.info("Node existed when trying to ensure path " + path + ", somebody beat us to it?");
      }
    }
  }

  /**
   * Validate and return a normalized zookeeper path which doesn't contain consecutive slashes and
   * never ends with a slash (except for root path).
   *
   * @param path the path to be normalized
   * @return normalized path string
   */
  public static String normalizePath(String path) {
    String normalizedPath = path.replaceAll("//+", "/").replaceFirst("(.+)/$", "$1");
    PathUtils.validatePath(normalizedPath);
    return normalizedPath;
  }

  private ZooKeeperUtils() {
    // utility
  }
}
