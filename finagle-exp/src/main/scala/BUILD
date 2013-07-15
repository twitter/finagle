scala_library(name='scala',
  dependencies=[
    pants('3rdparty:jsr166e'),
    pants('finagle/finagle-core'),
    pants('util/util-core'),
  ],
  sources=rglobs('com/twitter/finagle/exp/*.scala')
)

benchmark(name='benchmark',
  dependencies=[
    pants(':scala'),
    pants('3rdparty:caliper'),
    pants('util/util-core'),
  ],
  sources=rglobs('com/twitter/finagle/benchmark/*.scala')
)
