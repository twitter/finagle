**<server_label>/mux/draining**
  A counter of the number of times the server has initiated session draining.

**<server_label>/mux/leased** `verbosity:debug`
  A counter of the number of leases issued by the server when the session is
  is in a state other than draining or drained.

**<client_label>/mux/draining**
  A counter of the number of times a server initiated session draining.

**<client_label>/mux/drained**
  A counter of the number of times server-initiated draining completed
  successfully.

**<server_label>/mux/duplicate_tag**
  A counter of the number of requests with a tag while a server is
  processing another request with the same tag.

**<server_label>/mux/orphaned_tdiscard**
  A counter of the number of Tdiscard messages for which the server does
  not have a corresponding request.  This happens when a server has already
  responded to the request when it receives a Tdiscard.

**clienthangup**
  A counter of the number of times sessions have been abruptly terminated by
  the client.

**serverhangup**
  A counter of the number of times sessions have been abruptly terminated by
  the server.

**<label>/mux/framer/write_stream_bytes**
  A histogram of the number of bytes written to the transport when
  mux framing is enabled.

**<label>/mux/framer/read_stream_bytes**
  A histogram of the number of bytes read from the transport when
  mux framing is enabled.

**<label>/mux/framer/pending_write_streams**
  A gauge of the number of outstanding write streams when mux framing is enabled.

**<label>/mux/framer/pending_read_streams**
  A gauge of the number of outstanding read streams when mux framing is enabled.

**<label>/mux/framer/write_window_bytes**
  A gauge indicating the maximum size of fragments when mux framing is enabled.
  A value of -1 means that writes are not fragmented.

**<label>/mux/transport/read/failures/**
  A counter indicating any exceptions that occur on the transport read path for mux.
  This includes exceptions in handshaking, thrift downgrading (for servers), etc.

**<label>/mux/transport/write/failures/**
  A counter indicating any exceptions that occur on the transport write path for mux.
  This includes exceptions in handshaking, thrift downgrading (for servers), etc.

**<label>/mux/tls/upgrade/success**
  A counter of the number of times the client or server has successfully
  upgraded a connection to TLS.

**<label>/mux/tls/upgrade/incompatible**
  A counter of the number of times a client or server failed to establish a session
  due to incompatible TLS requirements or capabilities.

**<label>/mux/handshake_latency_us** `verbosity:debug`

  A histogram of the mux handshake latency.
