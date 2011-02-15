namespace java com.twitter.finagle.thrift

/** 
 * TODO: document this 
 */

struct TranscriptRecord {
  1: i32     host;
  2: string  vm_id;
  3: i64     span_id;
  4: i64     parent_span_id;
  5: i64     timestamp_ms;
  6: string  message;
}

struct TracedRequest {
  1: i64   parent_span_id;
  2: bool  debug;
}

struct TracedResponse {
  1: list<TranscriptRecord> transcript;
}

struct TraceOptions {}
