**<label>/redistributes**
  A counter of the number of times the cache ring has been rebuilt. This occurs
  whenever a node has been ejected or revived, or the set of nodes changes.

**<label>/joins**
  A counter of the number of times a node has been added to the cache ring because
  the backing set of servers has changed.

**<label/leaves>**
  A counter of the number of times a node has been removed from the cache ring
  because the backing set of servers has changed.

**<label>/ejections**
  A counter of the number of times a node has been ejected from the cache ring.

**<label>/revivals**
  A counter of the number of times an ejected node has been re-added to the cache ring.
