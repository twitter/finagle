Thrift/ThriftMux server side per-endpoint stats can be enabled by `.withPerEndpointStats`,
such as `ThriftMux.server.withPerEndpointStats...`

**<server_label>/<service_name>/<method_name>/requests**
  A counter of the total number of successes + failures for <method_name>.

**<server_label>/<service_name>/<method_name>/success**
  A counter of the total number of successes for <method_name>.

**<server_label>/<service_name>/<method_name>/failures/<exception_name>+**
  A counter of the number of times a specific exception has been thrown for <method_name>.

**<server_label>/<service_name>/<method_name>/failures**
  A counter of the number of times any failure has been observed for <method_name>.

Thrift/ThriftMux client side per-endpoint stats need to be enabled by `.withPerEndpointStats`
when constructing a client `ServicePerEndpoint`. Similar metrics as server side:

**<client_label>/<service_name>/<method_name>/requests**

**<client_label>/<service_name>/<method_name>/success**

**<client_label>/<service_name>/<method_name>/failures/<exception_name>+**

**<client_label>/<service_name>/<method_name>/failures**
