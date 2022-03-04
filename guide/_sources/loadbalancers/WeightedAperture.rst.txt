Weighted Aperture
-----------------

The weighted "flavor" of aperture load balancers was introduced as a means to
offer balancers more control over balancing traffic. Traditional aperture
balancers require preprocessing to generate groups of equally weighted 
endpoints, allowing the load balancer to send the same proportion of traffic
to each endpoint. Weighted aperture balancers, on the other hand, need no pre-processing. 
They have knowledge of endpoint weights and send traffic proportionally. Apart from 
allowing these balancers to behave more dynamically, this limits the number of connections
going to any single instance. For example, an endpoint with a unique weight, no matter
how small, would, in traditional balancers, end up in its own weight class alone, 
therefore appearing in every aperture and receiving many connections. With weighted 
aperture, that endpoint would simply occupy a smaller portion of the ring and 
therefore appear in fewer apertures. 
Though both Random and Deterministic Aperture can behave as weighted aperture balancers, 
their algorithms are slightly modified to account for this additional information.

Weighted Deterministic Aperture
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

With additional weight information, the ring model is no longer a simple abstraction 
for selecting endpoints to send traffic to. If endpoints can handle differing levels
of traffic, they can't take up equal portions of the ring. In other words, the
"fair die" feature of the ring is abandoned. The ring in the examples above might look
something like the following.


.. figure:: /_static/waperturering.png
   :width: 60%
   :align: center

Without some computation, it's not immediately obvious which endpoints are within
the aperture, nor is it easy to determine which endpoint a value between 0 and 1 
corresponds to, making random selection within an aperture equally difficult. 

Rather than representing the deterministic subsetting for weighted aperture as
overlapping rings, we shift to a two dimensional model that represent the nodes 
within a given aperture as reorganized areas. 

.. figure:: /_static/ring-to-area.gif
   :align: center

Mathematically, this computation is completed via `c.t.f.util.Drv <https://github.com/twitter/finagle/blob/develop/finagle-core/src/main/scala/com/twitter/finagle/util/Drv.scala>`__.
By adding a second dimension, we're able to equalize the widths of each endpoint while 
maintaining their weights in the form of areas. For example, an endpoint of weight 0.5
would, rather than having a length around a ring of 0.5, have a width of 1 and a height 
of 0.5, resulting in an overall area of 0.5. Crucially, this reorganization retains 
the O(1) complexity of the original ring model by making indexing simple. 

.. figure:: /_static/waperturearea.png
   :align: center

For more information on how determinism is maintained in weighted aperture balancers, 
check out `c.t.f.util.Drv <https://github.com/twitter/finagle/blob/develop/finagle-core/src/main/scala/com/twitter/finagle/util/Drv.scala>`__
and `this explanation <https://www.keithschwarz.com/darts-dice-coins/>`__ of the probabilistic models.

Weighted Random Aperture
^^^^^^^^^^^^^^^^^^^^^^^^

Similarly for random aperture, the problem of identifying endpoints when they're each
of different widths (or lengths) isn't as simple in the weighted pathway. As an added 
complexity, endpoints in random apertures are occasionally reorganized to ensure
healthy nodes are more often within a given aperture. Since building the Drv is costly, 
rebuilding it at unknown intervals is unreasonable. Therefore, for weighted random aperture,
we still arrange the endpoints along a ring bounded betweeen 0 and 1 but instead use binary 
search to determine which endpoint is selected by P2C. It's important 
to note that this change results in a shift from O(1) endpoint selection to O(log(N)) selection
complexity. This is a compromise made to ensure that the overall behavior of Random Aperture
is maintained.

Key Metrics
^^^^^^^^^^^

The key metrics for weighted apertures are the same as their traditional counterparts, with one 
exception. To determine if you're running a weighted balancer, check for the `_weighted` suffix
in the `$clientName/loadbalancer/algorithm` metric. 
