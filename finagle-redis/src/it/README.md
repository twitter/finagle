Finagle Redis Integration Tests
===============================

Running the finagle-redis integration tests is relatively straightforward. The
main thing to note is that they live in the `src/it` directory rather than the
`src/test` directory and so are run apart from the unit tests.

Instructions
------------

1. The integration tests require that redis be installed on your machine and
   that the `redis-server` binary be present. So, install redis. This can be
   done via https://redis.io/download or via `brew install redis`.

2. That's all the setup! To run with bazel, run
   `./bazel test finagle/finagle-redis/src/it/scala`. To run with sbt, run
   `./sbt 'project finagle-redis' 'it:testOnly'`.

Note that running the tests will create the file `dump.rdb` in the directory
from which `bazel` or `sbt` is run.
