package com.twitter.finagle.protobuf.rpc;


public abstract class WeatherService
        implements com.google.protobuf.Service {
    protected WeatherService() {}

    public interface Interface {
        public abstract void getWeatherForecast(
                com.google.protobuf.RpcController controller,
                com.twitter.finagle.protobuf.rpc.SampleServiceProtos.GetWeatherForecastRequest request,
                com.google.protobuf.RpcCallback<com.twitter.finagle.protobuf.rpc.SampleServiceProtos.GetWeatherForecastResponse> done);

        public abstract void getHistoricWeather(
                com.google.protobuf.RpcController controller,
                com.twitter.finagle.protobuf.rpc.SampleServiceProtos.GetHistoricWeatherRequest request,
                com.google.protobuf.RpcCallback<com.twitter.finagle.protobuf.rpc.SampleServiceProtos.GetHistoricWeatherResponse> done);

    }

    public static com.google.protobuf.Service newReflectiveService(
            final Interface impl) {
        return new WeatherService() {
            @java.lang.Override
            public  void getWeatherForecast(
                    com.google.protobuf.RpcController controller,
                    com.twitter.finagle.protobuf.rpc.SampleServiceProtos.GetWeatherForecastRequest request,
                    com.google.protobuf.RpcCallback<com.twitter.finagle.protobuf.rpc.SampleServiceProtos.GetWeatherForecastResponse> done) {
                impl.getWeatherForecast(controller, request, done);
            }

            @java.lang.Override
            public  void getHistoricWeather(
                    com.google.protobuf.RpcController controller,
                    com.twitter.finagle.protobuf.rpc.SampleServiceProtos.GetHistoricWeatherRequest request,
                    com.google.protobuf.RpcCallback<com.twitter.finagle.protobuf.rpc.SampleServiceProtos.GetHistoricWeatherResponse> done) {
                impl.getHistoricWeather(controller, request, done);
            }

        };
    }

    public static com.google.protobuf.BlockingService
    newReflectiveBlockingService(final BlockingInterface impl) {
        return new com.google.protobuf.BlockingService() {
            public final com.google.protobuf.Descriptors.ServiceDescriptor
            getDescriptorForType() {
                return getDescriptor();
            }

            public final com.google.protobuf.Message callBlockingMethod(
                    com.google.protobuf.Descriptors.MethodDescriptor method,
                    com.google.protobuf.RpcController controller,
                    com.google.protobuf.Message request)
                    throws com.google.protobuf.ServiceException {
                if (method.getService() != getDescriptor()) {
                    throw new java.lang.IllegalArgumentException(
                            "Service.callBlockingMethod() given method descriptor for " +
                                    "wrong service type.");
                }
                switch(method.getIndex()) {
                    case 0:
                        return impl.getWeatherForecast(controller, (com.twitter.finagle.protobuf.rpc.SampleServiceProtos.GetWeatherForecastRequest)request);
                    case 1:
                        return impl.getHistoricWeather(controller, (com.twitter.finagle.protobuf.rpc.SampleServiceProtos.GetHistoricWeatherRequest)request);
                    default:
                        throw new java.lang.AssertionError("Can't get here.");
                }
            }

            public final com.google.protobuf.Message
            getRequestPrototype(
                    com.google.protobuf.Descriptors.MethodDescriptor method) {
                if (method.getService() != getDescriptor()) {
                    throw new java.lang.IllegalArgumentException(
                            "Service.getRequestPrototype() given method " +
                                    "descriptor for wrong service type.");
                }
                switch(method.getIndex()) {
                    case 0:
                        return com.twitter.finagle.protobuf.rpc.SampleServiceProtos.GetWeatherForecastRequest.getDefaultInstance();
                    case 1:
                        return com.twitter.finagle.protobuf.rpc.SampleServiceProtos.GetHistoricWeatherRequest.getDefaultInstance();
                    default:
                        throw new java.lang.AssertionError("Can't get here.");
                }
            }

            public final com.google.protobuf.Message
            getResponsePrototype(
                    com.google.protobuf.Descriptors.MethodDescriptor method) {
                if (method.getService() != getDescriptor()) {
                    throw new java.lang.IllegalArgumentException(
                            "Service.getResponsePrototype() given method " +
                                    "descriptor for wrong service type.");
                }
                switch(method.getIndex()) {
                    case 0:
                        return com.twitter.finagle.protobuf.rpc.SampleServiceProtos.GetWeatherForecastResponse.getDefaultInstance();
                    case 1:
                        return com.twitter.finagle.protobuf.rpc.SampleServiceProtos.GetHistoricWeatherResponse.getDefaultInstance();
                    default:
                        throw new java.lang.AssertionError("Can't get here.");
                }
            }

        };
    }

    public abstract void getWeatherForecast(
            com.google.protobuf.RpcController controller,
            com.twitter.finagle.protobuf.rpc.SampleServiceProtos.GetWeatherForecastRequest request,
            com.google.protobuf.RpcCallback<com.twitter.finagle.protobuf.rpc.SampleServiceProtos.GetWeatherForecastResponse> done);

    public abstract void getHistoricWeather(
            com.google.protobuf.RpcController controller,
            com.twitter.finagle.protobuf.rpc.SampleServiceProtos.GetHistoricWeatherRequest request,
            com.google.protobuf.RpcCallback<com.twitter.finagle.protobuf.rpc.SampleServiceProtos.GetHistoricWeatherResponse> done);

    public static final
    com.google.protobuf.Descriptors.ServiceDescriptor
    getDescriptor() {
        return com.twitter.finagle.protobuf.rpc.SampleServiceProtos.getDescriptor().getServices().get(0);
    }
    public final com.google.protobuf.Descriptors.ServiceDescriptor
    getDescriptorForType() {
        return getDescriptor();
    }

    public final void callMethod(
            com.google.protobuf.Descriptors.MethodDescriptor method,
            com.google.protobuf.RpcController controller,
            com.google.protobuf.Message request,
            com.google.protobuf.RpcCallback<
                    com.google.protobuf.Message> done) {
        if (method.getService() != getDescriptor()) {
            throw new java.lang.IllegalArgumentException(
                    "Service.callMethod() given method descriptor for wrong " +
                            "service type.");
        }
        switch(method.getIndex()) {
            case 0:
                this.getWeatherForecast(controller, (com.twitter.finagle.protobuf.rpc.SampleServiceProtos.GetWeatherForecastRequest)request,
                        com.google.protobuf.RpcUtil.<com.twitter.finagle.protobuf.rpc.SampleServiceProtos.GetWeatherForecastResponse>specializeCallback(
                                done));
                return;
            case 1:
                this.getHistoricWeather(controller, (com.twitter.finagle.protobuf.rpc.SampleServiceProtos.GetHistoricWeatherRequest)request,
                        com.google.protobuf.RpcUtil.<com.twitter.finagle.protobuf.rpc.SampleServiceProtos.GetHistoricWeatherResponse>specializeCallback(
                                done));
                return;
            default:
                throw new java.lang.AssertionError("Can't get here.");
        }
    }

    public final com.google.protobuf.Message
    getRequestPrototype(
            com.google.protobuf.Descriptors.MethodDescriptor method) {
        if (method.getService() != getDescriptor()) {
            throw new java.lang.IllegalArgumentException(
                    "Service.getRequestPrototype() given method " +
                            "descriptor for wrong service type.");
        }
        switch(method.getIndex()) {
            case 0:
                return com.twitter.finagle.protobuf.rpc.SampleServiceProtos.GetWeatherForecastRequest.getDefaultInstance();
            case 1:
                return com.twitter.finagle.protobuf.rpc.SampleServiceProtos.GetHistoricWeatherRequest.getDefaultInstance();
            default:
                throw new java.lang.AssertionError("Can't get here.");
        }
    }

    public final com.google.protobuf.Message
    getResponsePrototype(
            com.google.protobuf.Descriptors.MethodDescriptor method) {
        if (method.getService() != getDescriptor()) {
            throw new java.lang.IllegalArgumentException(
                    "Service.getResponsePrototype() given method " +
                            "descriptor for wrong service type.");
        }
        switch(method.getIndex()) {
            case 0:
                return com.twitter.finagle.protobuf.rpc.SampleServiceProtos.GetWeatherForecastResponse.getDefaultInstance();
            case 1:
                return com.twitter.finagle.protobuf.rpc.SampleServiceProtos.GetHistoricWeatherResponse.getDefaultInstance();
            default:
                throw new java.lang.AssertionError("Can't get here.");
        }
    }

    public static Stub newStub(
            com.google.protobuf.RpcChannel channel) {
        return new Stub(channel);
    }

    public static final class Stub extends com.twitter.finagle.protobuf.rpc.WeatherService implements Interface {
        private Stub(com.google.protobuf.RpcChannel channel) {
            this.channel = channel;
        }

        private final com.google.protobuf.RpcChannel channel;

        public com.google.protobuf.RpcChannel getChannel() {
            return channel;
        }

        public  void getWeatherForecast(
                com.google.protobuf.RpcController controller,
                com.twitter.finagle.protobuf.rpc.SampleServiceProtos.GetWeatherForecastRequest request,
                com.google.protobuf.RpcCallback<com.twitter.finagle.protobuf.rpc.SampleServiceProtos.GetWeatherForecastResponse> done) {
            channel.callMethod(
                    getDescriptor().getMethods().get(0),
                    controller,
                    request,
                    com.twitter.finagle.protobuf.rpc.SampleServiceProtos.GetWeatherForecastResponse.getDefaultInstance(),
                    com.google.protobuf.RpcUtil.generalizeCallback(
                            done,
                            com.twitter.finagle.protobuf.rpc.SampleServiceProtos.GetWeatherForecastResponse.class,
                            com.twitter.finagle.protobuf.rpc.SampleServiceProtos.GetWeatherForecastResponse.getDefaultInstance()));
        }

        public  void getHistoricWeather(
                com.google.protobuf.RpcController controller,
                com.twitter.finagle.protobuf.rpc.SampleServiceProtos.GetHistoricWeatherRequest request,
                com.google.protobuf.RpcCallback<com.twitter.finagle.protobuf.rpc.SampleServiceProtos.GetHistoricWeatherResponse> done) {
            channel.callMethod(
                    getDescriptor().getMethods().get(1),
                    controller,
                    request,
                    com.twitter.finagle.protobuf.rpc.SampleServiceProtos.GetHistoricWeatherResponse.getDefaultInstance(),
                    com.google.protobuf.RpcUtil.generalizeCallback(
                            done,
                            com.twitter.finagle.protobuf.rpc.SampleServiceProtos.GetHistoricWeatherResponse.class,
                            com.twitter.finagle.protobuf.rpc.SampleServiceProtos.GetHistoricWeatherResponse.getDefaultInstance()));
        }
    }

    public static BlockingInterface newBlockingStub(
            com.google.protobuf.BlockingRpcChannel channel) {
        return new BlockingStub(channel);
    }

    public interface BlockingInterface {
        public com.twitter.finagle.protobuf.rpc.SampleServiceProtos.GetWeatherForecastResponse getWeatherForecast(
                com.google.protobuf.RpcController controller,
                com.twitter.finagle.protobuf.rpc.SampleServiceProtos.GetWeatherForecastRequest request)
                throws com.google.protobuf.ServiceException;

        public com.twitter.finagle.protobuf.rpc.SampleServiceProtos.GetHistoricWeatherResponse getHistoricWeather(
                com.google.protobuf.RpcController controller,
                com.twitter.finagle.protobuf.rpc.SampleServiceProtos.GetHistoricWeatherRequest request)
                throws com.google.protobuf.ServiceException;
    }

    private static final class BlockingStub implements BlockingInterface {
        private BlockingStub(com.google.protobuf.BlockingRpcChannel channel) {
            this.channel = channel;
        }

        private final com.google.protobuf.BlockingRpcChannel channel;

        public com.twitter.finagle.protobuf.rpc.SampleServiceProtos.GetWeatherForecastResponse getWeatherForecast(
                com.google.protobuf.RpcController controller,
                com.twitter.finagle.protobuf.rpc.SampleServiceProtos.GetWeatherForecastRequest request)
                throws com.google.protobuf.ServiceException {
            return (com.twitter.finagle.protobuf.rpc.SampleServiceProtos.GetWeatherForecastResponse) channel.callBlockingMethod(
                    getDescriptor().getMethods().get(0),
                    controller,
                    request,
                    com.twitter.finagle.protobuf.rpc.SampleServiceProtos.GetWeatherForecastResponse.getDefaultInstance());
        }


        public com.twitter.finagle.protobuf.rpc.SampleServiceProtos.GetHistoricWeatherResponse getHistoricWeather(
                com.google.protobuf.RpcController controller,
                com.twitter.finagle.protobuf.rpc.SampleServiceProtos.GetHistoricWeatherRequest request)
                throws com.google.protobuf.ServiceException {
            return (com.twitter.finagle.protobuf.rpc.SampleServiceProtos.GetHistoricWeatherResponse) channel.callBlockingMethod(
                    getDescriptor().getMethods().get(1),
                    controller,
                    request,
                    com.twitter.finagle.protobuf.rpc.SampleServiceProtos.GetHistoricWeatherResponse.getDefaultInstance());
        }

    }
}

