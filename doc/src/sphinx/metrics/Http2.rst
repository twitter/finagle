**<server_label>/upgrade/success**
  A counter of http2 upgrades and new prior knowledge connections server side.

**<client_label>/upgrade/success**
  A counter of http2 upgrades and new prior knowledge connections client side.

**<server_label>/upgrade/ignored**
  A counter of http2 upgrades that were ignored due to an unsuitable request.

**<client_label>/upgrade/ignored**
  A counter of http2 upgrades that were ignored due to an unsuitable request.

**<client_label>/upgrade/attempt**
  A counter of http2 upgrade attempts made by the client.

**<server_label>/streams**
  A gauge exposing the number of opened streams on the server.

**<client_label>/streams**
  A gauge exposing the number of opened streams on the client.

**<client_label>/buffered_streams**
  A gauge exposing the number of buffered streams on the client.

**<client_label>/dead_session**
  A counter of the number of closed sessions evicted.

**<client_label>/dead_child_transport**
  A counter of the number of child transports that were returned in a dead state.

**<client_label>closed_before_upgrade**
  A counter of the number of h2c upgrade transports that were closed before the
  upgrade had completed.
