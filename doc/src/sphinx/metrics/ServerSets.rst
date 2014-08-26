Stabilizing ServerSets
<<<<<<<<<<<<<<<<<<<<<<

Addresses that are resolved through ServerSets are stabilized in two ways:

1. ZooKeeper failures will not cause a previously bound address to fail.
2. When a member leaves a ServerSet, its removal is delayed.

Note that additions to a ServerSet are reflected immediately.

These metrics are scoped under the ServerSet's ZooKeeper path.

**limbo**

When a member leaves a ServerSet, it is placed in limbo. Hosts in limbo are still presented
as belonging to the ServerSet, but are being staged for removal, within an interval bound by
the ZooKeeper session timeout.

**size**

The current size of the live ServerSet, not including members in limbo.
