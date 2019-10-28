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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.annotation.Nullable;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.SessionExpiredException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.common.PathUtils;

import com.twitter.finagle.common.base.MorePreconditions;
import com.twitter.util.Duration;

/**
 * Manages a connection to a ZooKeeper cluster.
 */
public class ZooKeeperClient {

  /**
   * Indicates an error connecting to a zookeeper cluster.
   */
  public static class ZooKeeperConnectionException extends Exception {
    public ZooKeeperConnectionException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  /**
   * Encapsulates a user's credentials and has the ability to authenticate them through a
   * {@link ZooKeeper} client.
   */
  public interface Credentials {

    /**
     * A set of {@code Credentials} that performs no authentication.
     */
    Credentials NONE = new Credentials() {
      @Override public void authenticate(ZooKeeper zk) {
        // noop
      }

      @Override public String scheme() {
        return null;
      }

      @Override public byte[] authToken() {
        return null;
      }
    };

    /**
     * Authenticates these credentials against the given {@code ZooKeeper} client.
     *
     * @param zooKeeper the client to authenticate
     */
    void authenticate(ZooKeeper zooKeeper);

    /**
     * Returns the authentication scheme these credentials are for.
     *
     * @return the scheme these credentials are for or {@code null} if no authentication is
     *     intended.
     */
    @Nullable
    String scheme();

    /**
     * Returns the authentication token.
     *
     * @return the authentication token or {@code null} if no authentication is intended.
     */
    @Nullable
    byte[] authToken();
  }

  /**
   * Creates a set of credentials for the zoo keeper digest authentication mechanism.
   *
   * @param username the username to authenticate with
   * @param password the password to authenticate with
   * @return a set of credentials that can be used to authenticate the zoo keeper client
   */
  public static Credentials digestCredentials(String username, String password) {
    MorePreconditions.checkNotBlank(username);
    Objects.requireNonNull(password);

    // TODO(John Sirois): DigestAuthenticationProvider is broken - uses platform default charset
    // (on server) and so we just have to hope here that clients are deployed in compatible jvms.
    // Consider writing and installing a version of DigestAuthenticationProvider that controls its
    // Charset explicitly.
    return credentials("digest", (username + ":" + password).getBytes());
  }

  /**
   * Creates a set of credentials for the given authentication {@code scheme}.
   *
   * @param scheme the scheme to authenticate with
   * @param authToken the authentication token
   * @return a set of credentials that can be used to authenticate the zoo keeper client
   */
  public static Credentials credentials(final String scheme, final byte[] authToken) {
    MorePreconditions.checkNotBlank(scheme);
    Objects.requireNonNull(authToken);

    return new Credentials() {
      @Override public void authenticate(ZooKeeper zooKeeper) {
        zooKeeper.addAuthInfo(scheme, authToken);
      }

      @Override public String scheme() {
        return scheme;
      }

      @Override public byte[] authToken() {
        return authToken;
      }

      @Override public boolean equals(Object o) {
        if (!(o instanceof Credentials)) {
          return false;
        }

        Credentials other = (Credentials) o;
        return new EqualsBuilder()
            .append(scheme, other.scheme())
            .append(authToken, other.authToken())
            .isEquals();
      }

      @Override public int hashCode() {
        return Arrays.hashCode(new Object[] {scheme, authToken});
      }
    };
  }

  private final class SessionState {
    private final long sessionId;
    private final byte[] sessionPasswd;

    private SessionState(long sessionId, byte[] sessionPasswd) {
      this.sessionId = sessionId;
      this.sessionPasswd = sessionPasswd;
    }
  }

  private static final Logger LOG = Logger.getLogger(ZooKeeperClient.class.getName());

  private static final Duration WAIT_FOREVER = Duration.Zero();

  private final int sessionTimeoutMs;
  private final Credentials credentials;
  private final String zooKeeperServers;
  private final String connectString;
  // GuardedBy "this", but still volatile for tests, where we want to be able to see writes
  // made from within long synchronized blocks.
  private volatile ZooKeeper zooKeeper;
  private SessionState sessionState;

  private final Set<Watcher> watchers = new CopyOnWriteArraySet<Watcher>();
  private final BlockingQueue<WatchedEvent> eventQueue = new LinkedBlockingQueue<WatchedEvent>();

  private static Iterable<InetSocketAddress> combine(InetSocketAddress address,
      InetSocketAddress... addresses) {
    HashSet<InetSocketAddress> addrs = new HashSet<>(addresses.length + 1);
    addrs.add(address);
    addrs.addAll(Arrays.asList(addresses));
    return Collections.unmodifiableSet(addrs);
  }

  /**
   * Creates an unconnected client that will lazily attempt to connect on the first call to
   * {@link #get()}.
   *
   * @param sessionTimeout the ZK session timeout
   * @param zooKeeperServer the first, required ZK server
   * @param zooKeeperServers any additional servers forming the ZK cluster
   */
  public ZooKeeperClient(Duration sessionTimeout, InetSocketAddress zooKeeperServer,
      InetSocketAddress... zooKeeperServers) {
    this(sessionTimeout, combine(zooKeeperServer, zooKeeperServers));
  }

  /**
   * Creates an unconnected client that will lazily attempt to connect on the first call to
   * {@link #get}.
   *
   * @param sessionTimeout the ZK session timeout
   * @param zooKeeperServers the set of servers forming the ZK cluster
   */
  public ZooKeeperClient(Duration sessionTimeout,
      Iterable<InetSocketAddress> zooKeeperServers) {
    this(sessionTimeout, Credentials.NONE, Optional.empty(), zooKeeperServers);
  }

  /**
   * Creates an unconnected client that will lazily attempt to connect on the first call to
   * {@link #get()}.  All successful connections will be authenticated with the given
   * {@code credentials}.
   *
   * @param sessionTimeout the ZK session timeout
   * @param credentials the credentials to authenticate with
   * @param zooKeeperServer the first, required ZK server
   * @param zooKeeperServers any additional servers forming the ZK cluster
   */
  public ZooKeeperClient(Duration sessionTimeout, Credentials credentials,
      InetSocketAddress zooKeeperServer, InetSocketAddress... zooKeeperServers) {
    this(sessionTimeout, credentials, Optional.empty(),
         combine(zooKeeperServer, zooKeeperServers));
  }

  /**
   * Creates an unconnected client that will lazily attempt to connect on the first call to
   * {@link #get}.  All successful connections will be authenticated with the given
   * {@code credentials}.
   *
   * @param sessionTimeout the ZK session timeout
   * @param credentials the credentials to authenticate with
   * @param zooKeeperServers the set of servers forming the ZK cluster
   */
  public ZooKeeperClient(Duration sessionTimeout, Credentials credentials,
      Iterable<InetSocketAddress> zooKeeperServers) {
        this(sessionTimeout, credentials, Optional.empty(), zooKeeperServers);
      }

  /**
   * Creates an unconnected client that will lazily attempt to connect on the first call to
   * {@link #get}.  All successful connections will be authenticated with the given
   * {@code credentials}.
   *
   * @param sessionTimeout the ZK session timeout
   * @param credentials the credentials to authenticate with
   * @param chrootPath an optional chroot path
   * @param zooKeeperServers the set of servers forming the ZK cluster
   */
  public ZooKeeperClient(Duration sessionTimeout,
                         Credentials credentials,
                         Optional<String> chrootPath,
                         Iterable<InetSocketAddress> zooKeeperServers) {
    this.sessionTimeoutMs = (int) Objects.requireNonNull(sessionTimeout).inMillis();
    this.credentials = Objects.requireNonNull(credentials);

    if (chrootPath.isPresent()) {
      PathUtils.validatePath(chrootPath.get());
    }

    Objects.requireNonNull(zooKeeperServers);
    if (!zooKeeperServers.iterator().hasNext()) {
      throw new IllegalArgumentException("Must present at least 1 ZK server");
    }

    Thread watcherProcessor = new Thread("ZookeeperClient-watcherProcessor") {
      @Override
      public void run() {
        while (true) {
          try {
            WatchedEvent event = eventQueue.take();
            for (Watcher watcher : watchers) {
              watcher.process(event);
            }
          } catch (InterruptedException e) { /* ignore */ }
        }
      }
    };
    watcherProcessor.setDaemon(true);
    watcherProcessor.start();

    this.zooKeeperServers = StreamSupport.stream(zooKeeperServers.spliterator(), false)
        .map(addr -> addr.getHostName() + ":" + addr.getPort())
        .collect(Collectors.joining(","));
    this.connectString = this.zooKeeperServers.concat(chrootPath.orElse(""));
  }

  /**
   * Returns true if this client has non-empty credentials set.  For example, returns {@code false}
   * if this client was constructed with {@link Credentials#NONE}.
   *
   * @return {@code true} if this client is configured with non-empty credentials.
   */
  public boolean hasCredentials() {
    return credentials.scheme() != null
        && !credentials.scheme().isEmpty()
        && credentials.authToken() != null;
  }

  /**
   * Returns the current active ZK connection or establishes a new one if none has yet been
   * established or a previous connection was disconnected or had its session time out.  This method
   * will attempt to re-use sessions when possible.  Equivalent to:
   * <pre>get(Amount.of(0L, ...)</pre>.
   *
   * @return a connected ZooKeeper client
   * @throws ZooKeeperConnectionException if there was a problem connecting to the ZK cluster
   * @throws InterruptedException if interrupted while waiting for a connection to be established
   */
  public synchronized ZooKeeper get() throws ZooKeeperConnectionException, InterruptedException {
    try {
      return get(WAIT_FOREVER);
    } catch (TimeoutException e) {
      InterruptedException interruptedException =
          new InterruptedException("Got an unexpected TimeoutException for 0 wait");
      interruptedException.initCause(e);
      throw interruptedException;
    }
  }

  /**
   * Returns the current active ZK connection or establishes a new one if none has yet been
   * established or a previous connection was disconnected or had its session time out.  This
   * method will attempt to re-use sessions when possible.
   *
   * @param connectionTimeout the maximum amount of time to wait for the connection to the ZK
   *     cluster to be established; 0 to wait forever
   * @return a connected ZooKeeper client
   * @throws ZooKeeperConnectionException if there was a problem connecting to the ZK cluster
   * @throws InterruptedException if interrupted while waiting for a connection to be established
   * @throws TimeoutException if a connection could not be established within the configured
   *     session timeout
   */
  public synchronized ZooKeeper get(Duration connectionTimeout)
      throws ZooKeeperConnectionException, InterruptedException, TimeoutException {

    if (zooKeeper == null) {
      final CountDownLatch connected = new CountDownLatch(1);
      Watcher watcher = new Watcher() {
        @Override public void process(WatchedEvent event) {
          switch (event.getType()) {
            // Guard the None type since this watch may be used as the default watch on calls by
            // the client outside our control.
            case None:
              switch (event.getState()) {
                case Expired:
                  LOG.info("Zookeeper session expired. Event: " + event);
                  close();
                  break;
                case SyncConnected:
                  connected.countDown();
                  break;
                default:
                  break; // nop
              }
              break;
            default:
              break; // nop
          }

          eventQueue.offer(event);
        }
      };

      try {
        zooKeeper = (sessionState != null)
          ? new ZooKeeper(connectString, sessionTimeoutMs, watcher, sessionState.sessionId,
            sessionState.sessionPasswd)
          : new ZooKeeper(connectString, sessionTimeoutMs, watcher);
      } catch (IOException e) {
        throw new ZooKeeperConnectionException(
            "Problem connecting to servers: " + zooKeeperServers, e);
      }

      if (connectionTimeout.inMillis() > 0) {
        if (!connected.await(connectionTimeout.inMillis(), TimeUnit.MILLISECONDS)) {
          close();
          throw new TimeoutException("Timed out waiting for a ZK connection after "
                                     + connectionTimeout);
        }
      } else {
        try {
          connected.await();
        } catch (InterruptedException ex) {
          LOG.info("Interrupted while waiting to connect to zooKeeper");
          close();
          throw ex;
        }
      }
      credentials.authenticate(zooKeeper);

      sessionState = new SessionState(zooKeeper.getSessionId(), zooKeeper.getSessionPasswd());
    }
    return zooKeeper;
  }

  /**
   * Clients that need to re-establish state after session expiration can register an
   * {@code onExpired} command to execute.
   *
   * @param onExpired the {@code Runnable} to register
   * @return the new {@link Watcher} which can later be passed to {@link #unregister} for
   *     removal.
   */
  public Watcher registerExpirationHandler(final Runnable onExpired) {
    Watcher watcher = new Watcher() {
      @Override public void process(WatchedEvent event) {
        if (event.getType() == EventType.None && event.getState() == KeeperState.Expired) {
          onExpired.run();
        }
      }
    };
    register(watcher);
    return watcher;
  }

  /**
   * Clients that need to register a top-level {@code Watcher} should do so using this method.  The
   * registered {@code watcher} will remain registered across re-connects and session expiration
   * events.
   *
   * @param watcher the {@code Watcher to register}
   */
  public void register(Watcher watcher) {
    watchers.add(watcher);
  }

  /**
   * Clients can attempt to unregister a top-level {@code Watcher} that has previously been
   * registered.
   *
   * @param watcher the {@code Watcher} to unregister as a top-level, persistent watch
   * @return whether the given {@code Watcher} was found and removed from the active set
   */
  public boolean unregister(Watcher watcher) {
    return watchers.remove(watcher);
  }

  /**
   * Checks to see if the client might reasonably re-try an operation given the exception thrown
   * while attempting it.  If the ZooKeeper session should be expired to enable the re-try to
   * succeed this method will expire it as a side-effect.
   *
   * @param e the exception to test
   * @return true if a retry can be attempted
   */
  public boolean shouldRetry(KeeperException e) {
    if (e instanceof SessionExpiredException) {
      close();
    }
    return ZooKeeperUtils.isRetryable(e);
  }

  /**
   * Returns the comma separated list of host:port pairs used to connect to ZooKeeper.
   */
  public String getZooKeeperServers() {
    return zooKeeperServers;
  }

  /**
   * Returns the connect string
   */
  public String getConnectString() {
    return connectString;
  }

  /**
   * Closes the current connection if any expiring the current ZooKeeper session.  Any subsequent
   * calls to this method will no-op until the next successful {@link #get}.
   */
  public synchronized void close() {
    if (zooKeeper != null) {
      try {
        zooKeeper.close();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOG.warning("Interrupted trying to close zooKeeper");
      } finally {
        zooKeeper = null;
        sessionState = null;
      }
    }
  }

  /** VisibleForTesting */
  synchronized boolean isClosed() {
    return zooKeeper == null;
  }

  /** VisibleForTesting */
  ZooKeeper getZooKeeperClientForTests() {
    return zooKeeper;
  }
}
