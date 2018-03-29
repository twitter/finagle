package com.twitter.finagle.common.zookeeper;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.function.Function;

import com.twitter.finagle.common.io.Codec;
import com.twitter.thrift.Endpoint;
import com.twitter.thrift.ServiceInstance;
import com.twitter.thrift.Status;

/**
 * Common ServerSet related functions
 */
public final class ServerSets {

  private ServerSets() {
    // Utility class.
  }

  /**
   * A function that invokes {@link #toEndpoint(InetSocketAddress)}.
   */
  public static final Function<InetSocketAddress, Endpoint> TO_ENDPOINT = ServerSets::toEndpoint;

  /**
   * Returns a serialized Thrift service instance object, with given endpoints and codec.
   *
   * @param serviceInstance the Thrift service instance object to be serialized
   * @param codec the codec to use to serialize a Thrift service instance object
   * @return byte array that contains a serialized Thrift service instance
   */
  public static byte[] serializeServiceInstance(
      ServiceInstance serviceInstance, Codec<ServiceInstance> codec) throws IOException {

    ByteArrayOutputStream output = new ByteArrayOutputStream();
    codec.serialize(serviceInstance, output);
    return output.toByteArray();
  }

  /**
   * Serializes a service instance based on endpoints.
   * @see #serializeServiceInstance(ServiceInstance, Codec)
   *
   * @param address the target address of the service instance
   * @param additionalEndpoints additional endpoints of the service instance
   * @param status service status
   */
  public static byte[] serializeServiceInstance(
      InetSocketAddress address,
      Map<String, Endpoint> additionalEndpoints,
      Status status,
      Codec<ServiceInstance> codec) throws IOException {

    ServiceInstance serviceInstance =
        new ServiceInstance(toEndpoint(address), additionalEndpoints, status);
    return serializeServiceInstance(serviceInstance, codec);
  }

  /**
   * Creates a service instance object deserialized from byte array.
   *
   * @param data the byte array contains a serialized Thrift service instance
   * @param codec the codec to use to deserialize the byte array
   */
  public static ServiceInstance deserializeServiceInstance(
      byte[] data, Codec<ServiceInstance> codec) throws IOException {

    return codec.deserialize(new ByteArrayInputStream(data));
  }

  /**
   * Creates an endpoint for the given InetSocketAddress.
   *
   * @param address the target address to create the endpoint for
   */
  public static Endpoint toEndpoint(InetSocketAddress address) {
    return new Endpoint(address.getHostString(), address.getPort());
  }
}
