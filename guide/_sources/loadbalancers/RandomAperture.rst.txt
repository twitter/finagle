Random Aperture
---------------

The Random Aperture load balancer was developed to reduce the number of connections that clients
will establish when talking to large clusters. It does so by selecting a random subset of backends
to talk do under normal conditions with some slight changes under extraordinary conditions. This a
positive effect for both clients and servers by reducing the connections between them but comes with
the cost of load banding. Because clients choose which servers they talk randomly, it's possible
that some choices of the minimum number of peers to talk to will cause some servers to have more
clients than others. Assuming that clients in a cluster produce the same amount of load, this means
that some servers will have more or less load than others. This makes it harder to reason about a
clusters overall capacity, overload scenarios, and failure modes because servers are no longer
uniformly loaded.

.. figure:: /_static/random-aperture.png
   :align: center

   Service A connecting to Service B using random aperture. The number of connections are reduced
   but there is an uneven number of connections per service B host due to the statistical nature of
   service A picking hosts randomly.

Minimizing Banding in Random Aperture
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The random aperture load distribution can be very closely approximated by the binomial distribution.
The distribution represents the likelihood that a server will have a given number of clients, where
a client represents a uniform amount of load. A binomial distribution can be characterized by two
parameters, n and p, where n is the number of edges (trials) between the clients and the servers,
and p is the probability that a given server is chosen when a client chooses an edge. In a
client/server relationship,

::

   n = (# client) * (# remote peers per client)
   p = 1 / (# servers)

The distribution can be used as a guide for how to choose the number of remote peers per client.
Since the binomial distribution starts to turn Gaussian as the increase the number of trials,
properties of Gaussian distributions can be used to reason about the binomial distribution (note
that it's worth checking that the graph looks reasonably Gaussian before using these rules of
thumb). In a Gaussian distribution, approximately 1/3 of the servers will be < (mean - stddev), and
approximately 1/3 of the servers will be > (mean + stddev). This can be used to extrapolate the
distribution of connections per host. Wolfram Alpha is a good resource for visualizing the behavior
of a binomial distribution for different parameters.

Example
^^^^^^^

Let's say that there exists a service called "Brooklyn" that has 1000 nodes in its cluster which is
configured to use the aperture load balancer to connect with a service consisting of 5000 nodes
called "GadgetDuck". The goal is to pick the minimum number of connections that will result in a
~20% or less difference between the low and high load bands. As an initial value for exploring the
distributions, the asymptotic value of 1 is used as the aperture size. Using `Wolfram Alpha
<https://www.wolframalpha.com/input/?i=binomial+distribution(5000,+0.001)>`_, the mean is found
to be 5, and the stddev will be 2.5. This distribution is too wide to satisfy the 20% difference
requirement, so the next step is to increase the number of remote peers to 10. `Wolfram
Alpha <https://www.wolframalpha.com/input/?i=binomial+distribution(50000,+0.001)>`_ computes the mean
and stdev of the new distribution to be 50 and 7, respectively, which is still too wide. `A third
attempt <https://www.wolframalpha.com/input/?i=binomial+distribution(500000,+0.001)>`_ using 100
remote peers yields a mean of 500 and stddev of 22 which is within the requirements. Thus, an
aperture size of 100 or more will satisfy the load banding requirement. Further iterations
could be used to shrink the aperture size to a value in the range (10, 100] that still satisfies
the banding requirement, if so desired.
