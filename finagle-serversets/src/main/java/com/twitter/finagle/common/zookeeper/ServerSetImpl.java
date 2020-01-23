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
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;

import com.google.gson.Gson;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;

import com.twitter.finagle.common.base.ExceptionalSupplier;
import com.twitter.finagle.common.io.Codec;
import com.twitter.finagle.common.util.BackoffHelper;
import com.twitter.finagle.common.zookeeper.Group.GroupChangeListener;
import com.twitter.finagle.common.zookeeper.Group.JoinException;
import com.twitter.finagle.common.zookeeper.Group.Membership;
import com.twitter.finagle.common.zookeeper.Group.WatchException;
import com.twitter.finagle.common.zookeeper.ZooKeeperClient.ZooKeeperConnectionException;
import com.twitter.thrift.Endpoint;
import com.twitter.thrift.ServiceInstance;
import com.twitter.thrift.Status;

/**
 * ZooKeeper-backed implementation of {@link ServerSet}.
 */
public class ServerSetImpl implements ServerSet {
  private static final Logger LOG = Logger.getLogger(ServerSetImpl.class.getName());

  private final ZooKeeperClient zkClient;
  private final Group group;
  private final Codec<ServiceInstance> codec;
  private final BackoffHelper backoffHelper;

  /**
   * Creates a new ServerSet using open ZooKeeper node ACLs.
   *
   * @param zkClient the client to use for interactions with ZooKeeper
   * @param path the name-service path of the service to connect to
   */
  public ServerSetImpl(ZooKeeperClient zkClient, String path) {
    this(zkClient, ZooDefs.Ids.OPEN_ACL_UNSAFE, path);
  }

  /**
   * Creates a new ServerSet for the given service {@code path}.
   *
   * @param zkClient the client to use for interactions with ZooKeeper
   * @param acl the ACL to use for creating the persistent group path if it does not already exist
   * @param path the name-service path of the service to connect to
   */
  public ServerSetImpl(ZooKeeperClient zkClient, Iterable<ACL> acl, String path) {
    this(zkClient, new Group(zkClient, acl, path), createCodec());
  }

  /**
   * Creates a new ServerSet using the given service {@code group}.
   *
   * @param zkClient the client to use for interactions with ZooKeeper
   * @param group the server group
   */
  public ServerSetImpl(ZooKeeperClient zkClient, Group group) {
    this(zkClient, group, createCodec());
  }

  /**
   * Creates a new ServerSet using the given service {@code group} and a custom {@code codec}.
   *
   * @param zkClient the client to use for interactions with ZooKeeper
   * @param group the server group
   * @param codec a codec to use for serializing and de-serializing the ServiceInstance data to and
   *     from a byte array
   */
  public ServerSetImpl(ZooKeeperClient zkClient, Group group, Codec<ServiceInstance> codec) {
    this.zkClient = Objects.requireNonNull(zkClient);
    this.group = Objects.requireNonNull(group);
    this.codec = Objects.requireNonNull(codec);

    // TODO(John Sirois): Inject the helper so that backoff strategy can be configurable.
    backoffHelper = new BackoffHelper();
  }

  /** VisibleForTesting */
  ZooKeeperClient getZkClient() {
    return zkClient;
  }

  @Override
  public EndpointStatus join(
      InetSocketAddress endpoint,
      Map<String, InetSocketAddress> additionalEndpoints)
      throws JoinException, InterruptedException {

    LOG.log(Level.WARNING,
        "Joining a ServerSet without a shard ID is deprecated and will soon break.");
    return join(endpoint, additionalEndpoints, Optional.empty(), new HashMap<>());
  }

  @Override
  public EndpointStatus join(
      InetSocketAddress endpoint,
      Map<String, InetSocketAddress> additionalEndpoints,
      int shardId) throws JoinException, InterruptedException {
    return join(endpoint, additionalEndpoints, Optional.of(shardId), new HashMap<>());
  }

  @Override
  public EndpointStatus join(
      InetSocketAddress endpoint,
      Map<String, InetSocketAddress> additionalEndpoints,
      int shardId,
      Map<String, String> metadata) throws JoinException, InterruptedException {
    return join(endpoint, additionalEndpoints, Optional.of(shardId), metadata);
  }

  private EndpointStatus join(
      InetSocketAddress endpoint,
      Map<String, InetSocketAddress> additionalEndpoints,
      Optional<Integer> shardId,
      Map<String, String> metadata) throws JoinException, InterruptedException {

    Objects.requireNonNull(endpoint);
    Objects.requireNonNull(additionalEndpoints);

    final MemberStatus memberStatus =
        new MemberStatus(endpoint, additionalEndpoints, shardId, metadata);
    Supplier<byte[]> serviceInstanceSupplier = new Supplier<byte[]>() {
      @Override public byte[] get() {
        return memberStatus.serializeServiceInstance();
      }
    };
    final Membership membership = group.join(serviceInstanceSupplier);

    return new EndpointStatus() {
      @Override public void update(Status status) throws UpdateException {
        Objects.requireNonNull(status);
        LOG.warning("This method is deprecated. Please use leave() instead.");
        if (status == Status.DEAD) {
          leave();
        } else {
          LOG.warning("Status update has been ignored");
        }
      }

      @Override public void leave() throws UpdateException {
        memberStatus.leave(membership);
      }
    };
  }

  @Override
  public EndpointStatus join(
      InetSocketAddress endpoint,
      Map<String, InetSocketAddress> additionalEndpoints,
      Status status) throws JoinException, InterruptedException {

    LOG.warning("This method is deprecated. Please do not specify a status field.");
    if (status != Status.ALIVE) {
      LOG.severe("**************************************************************************\n"
          + "WARNING: MUTABLE STATUS FIELDS ARE NO LONGER SUPPORTED.\n"
          + "JOINING WITH STATUS ALIVE EVEN THOUGH YOU SPECIFIED " + status
          + "\n**************************************************************************");
    }
    return join(endpoint, additionalEndpoints);
  }

  @Override
  public Runnable watch(HostChangeMonitor<ServiceInstance> monitor) throws MonitorException {
    ServerSetWatcher serverSetWatcher = new ServerSetWatcher(zkClient, monitor);
    try {
      return serverSetWatcher.watch();
    } catch (WatchException e) {
      throw new MonitorException("ZooKeeper watch failed.", e);
    } catch (InterruptedException e) {
      throw new MonitorException("Interrupted while watching ZooKeeper.", e);
    }
  }

  @Override
  public void monitor(HostChangeMonitor<ServiceInstance> monitor) throws MonitorException {
    LOG.warning("This method is deprecated. Please use watch instead.");
    watch(monitor);
  }

  private final class MemberStatus {
    private final InetSocketAddress endpoint;
    private final Map<String, InetSocketAddress> additionalEndpoints;
    private final Optional<Integer> shardId;
    private final Map<String, String> metadata;

    private MemberStatus(
        InetSocketAddress endpoint,
        Map<String, InetSocketAddress> additionalEndpoints,
        Optional<Integer> shardId,
        Map<String, String> metadata) {

      this.endpoint = endpoint;
      this.additionalEndpoints = additionalEndpoints;
      this.shardId = shardId;
      this.metadata = metadata;
    }

    synchronized void leave(Membership membership) throws UpdateException {
      try {
        membership.cancel();
      } catch (JoinException e) {
        throw new UpdateException(
            "Failed to auto-cancel group membership on transition to DEAD status", e);
      }
    }

    byte[] serializeServiceInstance() {
      ServiceInstance serviceInstance = new ServiceInstance(
          ServerSets.toEndpoint(endpoint),
          additionalEndpoints.entrySet().stream().collect(Collectors.toMap(
            Map.Entry::getKey,
            e -> ServerSets.TO_ENDPOINT.apply(e.getValue()))),
          Status.ALIVE);

      if (shardId.isPresent()) {
        serviceInstance.setShard(shardId.get());
      }

      if (metadata != null && !metadata.isEmpty()) {
        serviceInstance.setMetadata(metadata);
      }

      LOG.fine("updating endpoint data to:\n\t" + serviceInstance);
      try {
        return ServerSets.serializeServiceInstance(serviceInstance, codec);
      } catch (IOException e) {
        throw new IllegalStateException("Unexpected problem serializing thrift struct "
            + serviceInstance + "to a byte[]", e);
      }
    }
  }

  private static class ServiceInstanceFetchException extends RuntimeException {
    ServiceInstanceFetchException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  private static class ServiceInstanceDeletedException extends RuntimeException {
    ServiceInstanceDeletedException(String path) {
      super(path);
    }
  }

  private static <E> Set<E> setDifference(final Set<E> set1, final Set<E> set2) {
    return set1.stream()
      .filter(e -> !set2.contains(e))
      .collect(Collectors.toSet());
  }

  private class ServerSetWatcher {
    private final ZooKeeperClient zkClient;
    private final HostChangeMonitor<ServiceInstance> monitor;
    @Nullable private Set<ServiceInstance> serverSet;

    ServerSetWatcher(ZooKeeperClient zkClient, HostChangeMonitor<ServiceInstance> monitor) {
      this.zkClient = zkClient;
      this.monitor = monitor;
    }

    public Runnable watch() throws WatchException, InterruptedException {
      Watcher onExpirationWatcher = zkClient.registerExpirationHandler(new Runnable() {
        @Override public void run() {
          // Servers may have changed Status while we were disconnected from ZooKeeper, check and
          // re-register our node watches.
          rebuildServerSet();
        }
      });

      try {
        return group.watch(new GroupChangeListener() {
          @Override public void onGroupChange(Iterable<String> memberIds) {
            notifyGroupChange(memberIds);
          }
        });
      } catch (WatchException e) {
        zkClient.unregister(onExpirationWatcher);
        throw e;
      } catch (InterruptedException e) {
        zkClient.unregister(onExpirationWatcher);
        throw e;
      }
    }

    private ServiceInstance getServiceInstance(final String nodePath) {
      try {
        return backoffHelper.doUntilResult(
          new ExceptionalSupplier<ServiceInstance, RuntimeException>() {
            @Override public ServiceInstance get() {
              try {
                byte[] data = zkClient.get().getData(nodePath, false, null);
                return ServerSets.deserializeServiceInstance(data, codec);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new ServiceInstanceFetchException(
                    "Interrupted updating service data for: " + nodePath, e);
              } catch (ZooKeeperConnectionException e) {
                LOG.log(Level.WARNING,
                    "Temporary error trying to updating service data for: " + nodePath, e);
                return null;
              } catch (NoNodeException e) {
                invalidateNodePath(nodePath);
                throw new ServiceInstanceDeletedException(nodePath);
              } catch (KeeperException e) {
                if (zkClient.shouldRetry(e)) {
                  LOG.log(Level.WARNING,
                      "Temporary error trying to update service data for: " + nodePath, e);
                  return null;
                } else {
                  throw new ServiceInstanceFetchException(
                      "Failed to update service data for: " + nodePath, e);
                }
              } catch (IOException e) {
                throw new ServiceInstanceFetchException(
                    "Failed to deserialize the ServiceInstance data for: " + nodePath, e);
              }
            }
        });
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new ServiceInstanceFetchException(
            "Interrupted trying to update service data for: " + nodePath, e);
      }
    }

    private final LoadingCache<String, ServiceInstance> servicesByMemberId =
        Caffeine.newBuilder().build(memberId ->
            getServiceInstance(group.getMemberPath(memberId)));

    private void rebuildServerSet() {
      Set<String> memberIds =
          Collections.unmodifiableSet(new HashSet<>(servicesByMemberId.asMap().keySet()));
      servicesByMemberId.invalidateAll();
      notifyGroupChange(memberIds);
    }

    private String invalidateNodePath(String deletedPath) {
      String memberId = group.getMemberId(deletedPath);
      servicesByMemberId.invalidate(memberId);
      return memberId;
    }

    private final Function<String, ServiceInstance> maybeFetchNode = memberId -> {
      // This get will trigger a fetch
      try {
        return servicesByMemberId.get(memberId);
      } catch (CompletionException e) {
        Throwable cause = e.getCause();
        if (!(cause instanceof ServiceInstanceDeletedException)) {
          if (ServiceInstanceFetchException.class.isInstance(cause)) {
            throw ServiceInstanceFetchException.class.cast(cause);
          } else {
            throw new IllegalStateException(
              "Unexpected error fetching member data for: " + memberId, e);
          }
        }
        return null;
      }
    };

    private synchronized void notifyGroupChange(Iterable<String> memberIds) {
      Set<String> newMemberIds = new HashSet<>();
      memberIds.iterator().forEachRemaining(newMemberIds::add);
      Set<String> existingMemberIds = servicesByMemberId.asMap().keySet();

      // Ignore no-op state changes except for the 1st when we've seen no group yet.
      if ((serverSet == null) || !newMemberIds.equals(existingMemberIds)) {
        Set<String> deletedMemberIds = setDifference(existingMemberIds, newMemberIds);
        // Implicit removal from servicesByMemberId.
        existingMemberIds.removeAll(deletedMemberIds);

        Set<ServiceInstance> serviceInstances = newMemberIds.stream()
            .map(maybeFetchNode)
            .filter(Objects::nonNull)
            .collect(Collectors.toSet());
        notifyServerSetChange(Collections.unmodifiableSet(serviceInstances));
      }
    }

    private void notifyServerSetChange(Set<ServiceInstance> currentServerSet) {
      // ZK nodes may have changed if there was a session expiry for a server in the server set, but
      // if the server's status has not changed, we can skip any onChange updates.
      if (!currentServerSet.equals(serverSet)) {
        if (currentServerSet.isEmpty()) {
          LOG.warning("server set empty for path " + group.getPath());
        } else {
          if (LOG.isLoggable(Level.INFO)) {
            if (serverSet == null) {
              LOG.info("received initial membership " + currentServerSet);
            } else {
              logChange(Level.INFO, currentServerSet);
            }
          }
        }
        serverSet = currentServerSet;
        monitor.onChange(serverSet);
      }
    }

    private void logChange(Level level, Set<ServiceInstance> newServerSet) {
      StringBuilder message = new StringBuilder("server set " + group.getPath() + " change: ");
      if (serverSet.size() != newServerSet.size()) {
        message.append("from ").append(serverSet.size())
            .append(" members to ").append(newServerSet.size());
      }

      Set<ServiceInstance> left = setDifference(serverSet, newServerSet);
      if (!left.isEmpty()) {
        String gone = left.stream()
            .map(Objects::toString)
            .collect(Collectors.joining("\n\t\t"));
        message.append("\n\tleft:\n\t\t").append(gone);
      }

      Set<ServiceInstance> joined = setDifference(newServerSet, serverSet);
      if (!joined.isEmpty()) {
        String added = joined.stream()
            .map(Objects::toString)
            .collect(Collectors.joining("\n\t\t"));
        message.append("\n\tjoined:\n\t\t").append(added);
      }

      LOG.log(level, message.toString());
    }
  }

  private static class EndpointSchema {
    private final String host;
    private final Integer port;

    EndpointSchema(Endpoint endpoint) {
      Objects.requireNonNull(endpoint);
      this.host = endpoint.getHost();
      this.port = endpoint.getPort();
    }

    String getHost() {
      return host;
    }

    Integer getPort() {
      return port;
    }
  }

  private static class ServiceInstanceSchema {
    private final EndpointSchema serviceEndpoint;
    private final Map<String, EndpointSchema> additionalEndpoints;
    private final Status status;
    private final Integer shard;
    private final Map<String, String> metadata;

    ServiceInstanceSchema(ServiceInstance instance) {
      this.serviceEndpoint = new EndpointSchema(instance.getServiceEndpoint());
      if (instance.getAdditionalEndpoints() != null) {
        this.additionalEndpoints = instance.getAdditionalEndpoints().entrySet().stream()
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                e -> new EndpointSchema(e.getValue()))
            );
      } else {
        this.additionalEndpoints = new HashMap<>();
      }
      this.status  = instance.getStatus();
      this.shard = instance.isSetShard() ? instance.getShard() : null;
      this.metadata = instance.isSetMetadata() ? instance.getMetadata() : null;
    }

    EndpointSchema getServiceEndpoint() {
      return serviceEndpoint;
    }

    Map<String, EndpointSchema> getAdditionalEndpoints() {
      return additionalEndpoints;
    }

    Status getStatus() {
      return status;
    }

    Integer getShard() {
      return shard;
    }

    Map<String, String> getMetadata() {
      return metadata;
    }
  }

  /**
   * An adapted JSON codec that makes use of {@link ServiceInstanceSchema} to circumvent the
   * __isset_bit_vector internal thrift struct field that tracks primitive types.
   */
  private static class AdaptedJsonCodec implements Codec<ServiceInstance> {
    private static final Charset ENCODING = StandardCharsets.UTF_8;
    private static final Class<ServiceInstanceSchema> CLASS = ServiceInstanceSchema.class;
    private final Gson gson = new Gson();

    @Override
    public void serialize(ServiceInstance instance, OutputStream sink) throws IOException {
      Writer w = new OutputStreamWriter(sink, ENCODING);
      gson.toJson(new ServiceInstanceSchema(instance), CLASS, w);
      w.flush();
    }

    @Override
    public ServiceInstance deserialize(InputStream source) throws IOException {
      ServiceInstanceSchema output = gson.fromJson(new InputStreamReader(source, ENCODING), CLASS);
      Endpoint primary = new Endpoint(
          output.getServiceEndpoint().getHost(), output.getServiceEndpoint().getPort());
      Map<String, Endpoint> additional = output.getAdditionalEndpoints().entrySet().stream()
          .collect(Collectors.toMap(
              Map.Entry::getKey,
              e -> new Endpoint(e.getValue().getHost(), e.getValue().getPort()))
          );
      ServiceInstance instance =
          new ServiceInstance(primary, Collections.unmodifiableMap(additional), output.getStatus());
      if (output.getShard() != null) {
        instance.setShard(output.getShard());
      }
      if (output.getMetadata() != null) {
        instance.setMetadata(output.getMetadata());
      }
      return instance;
    }
  }

  private static Codec<ServiceInstance> createCodec() {
    return new AdaptedJsonCodec();
  }
}
