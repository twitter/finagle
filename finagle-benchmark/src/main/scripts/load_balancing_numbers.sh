#!/usr/bin/env bash

./sbt 'project finagle-benchmark' \
      'run-main com.twitter.finagle.loadbalancer.Simulation -dur=60.seconds -bal=rr -qps=1000 -showprogress=false -showsummary=false -coldstart=false' 2>&1 |gawk 'match($0, /BinaryAnnotation\(balancer,([0-9]+).*\)/, a){print a[1]}' > all_good_1000_rr.txt

./sbt 'project finagle-benchmark' \
      'run-main com.twitter.finagle.loadbalancer.Simulation -dur=60.seconds -bal=p2c -qps=1000 -showprogress=false -showsummary=false -coldstart=false' 2>&1 |gawk 'match($0, /BinaryAnnotation\(balancer,([0-9]+).*\)/, a){print a[1]}' > all_good_1000_p2c.txt

./sbt 'project finagle-benchmark' \
      'run-main com.twitter.finagle.loadbalancer.Simulation -dur=60.seconds -bal=ewma -qps=1000 -showprogress=false -showsummary=false -coldstart=false' 2>&1 |gawk 'match($0, /BinaryAnnotation\(balancer,([0-9]+).*\)/, a){print a[1]}' > all_good_1000_ewma.txt

./sbt 'project finagle-benchmark' \
      'run-main com.twitter.finagle.loadbalancer.Simulation -dur=60.seconds -bal=rr -qps=1000 -showprogress=false -showsummary=false -coldstart=true' 2>&1 |gawk 'match($0, /BinaryAnnotation\(balancer,([0-9]+).*\)/, a){print a[1]}' > slow_start_1000_rr.txt

./sbt 'project finagle-benchmark' \
      'run-main com.twitter.finagle.loadbalancer.Simulation -dur=60.seconds -bal=p2c -qps=1000 -showprogress=false -showsummary=false -coldstart=true' 2>&1 |gawk 'match($0, /BinaryAnnotation\(balancer,([0-9]+).*\)/, a){print a[1]}' > slow_start_1000_p2c.txt

./sbt 'project finagle-benchmark' \
'run-main com.twitter.finagle.loadbalancer.Simulation -bal=ewma -dur=60.seconds -qps=1000 -showprogress=false -showsummary=false -coldstart=true' 2>&1 |gawk 'match($0, /BinaryAnnotation\(balancer,([0-9]+).*\)/, a){print a[1]}' > slow_start_1000_ewma.txt

./sbt 'project finagle-benchmark' \
      'run-main com.twitter.finagle.loadbalancer.Simulation -dur=60.seconds -bal=rr -qps=1000 -showprogress=false -showsummary=false -coldstart=false -slowmiddle=true' 2>&1 |gawk 'match($0, /BinaryAnnotation\(balancer,([0-9]+).*\)/, a){print a[1]}' > slow_middle_1000_rr.txt

./sbt 'project finagle-benchmark' \
      'run-main com.twitter.finagle.loadbalancer.Simulation -dur=60.seconds -bal=p2c -qps=1000 -showprogress=false -showsummary=false -coldstart=false -slowmiddle=true' 2>&1 |gawk 'match($0, /BinaryAnnotation\(balancer,([0-9]+).*\)/, a){print a[1]}' > slow_middle_1000_p2c.txt

./sbt 'project finagle-benchmark' \
      'run-main com.twitter.finagle.loadbalancer.Simulation -dur=60.seconds -bal=ewma -qps=1000 -showprogress=false -showsummary=false -coldstart=false -slowmiddle=true' 2>&1 |gawk 'match($0, /BinaryAnnotation\(balancer,([0-9]+).*\)/, a){print a[1]}' > slow_middle_1000_ewma.txt
