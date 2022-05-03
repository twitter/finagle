This is not really an R script, it is a literate transcript of the R
functions I wrote to generate a graph combined with some commentary on
the data generated. How I use this script is to copy and paste
snippets into my R workspace.

# returns a data.frame with response times from different load balancers, sampled at the given rate.
buildData <- function(rr, p2c, ewma, samples=1000) {
    rr <- read.table(rr)
    p2c <- read.table(p2c)
    ewma <- read.table(ewma)
    data = data.frame(seq(1, samples),
         sample(rr$V1, samples),
         sample(p2c$V1, samples),
         sample(ewma$V1, samples))

    names(data) <- c("id", "rr", "p2c", "ewma")

    return(data)
}

allGood <- buildData(rr="all_good_1000_rr.txt", p2c="all_good_1000_p2c.txt", ewma="all_good_1000_ewma.txt")

slowMiddle <- buildData(rr="slow_middle_1000_rr.txt", p2c="slow_middle_1000_p2c.txt", ewma="slow_middle_1000_ewma.txt")

slowStart <- buildData(rr="slow_start_1000_rr.txt", p2c="slow_start_1000_p2c.txt", ewma="slow_start_1000_ewma.txt")

# Overlapping ewma vs rr histograms
> nines = c(0.5, 0.9, 0.95, 0.96, 0.97, 0.98, 0.99, 0.999, 0.9999, 0.99999)
> quantile(slowMiddle$p2c, nines)
     50%      90%      95%      96%      97%      98%      99%    99.9%   99.99%  99.999%
 174.000  184.000  189.000  191.000  193.000  197.000  205.000 1723.004 1825.201 1907.291
> quantile(slowMiddle$ewma, nines)
     50%      90%      95%      96%      97%      98%      99%    99.9%   99.99%  99.999%
 174.000  183.000  188.000  189.000  191.000  194.000  199.000  211.000 1780.103 1819.110
> quantile(slowMiddle$rr, nines)
    50%     90%     95%     96%     97%     98%     99%   99.9%  99.99% 99.999%
 175.00  187.00  203.00 1664.00 1673.00 1683.00 1714.00 1862.00 1977.70 1994.56

### How to think about the latency graphic where Round Robin jumps up
to 2 seconds after 95%

What does it mean for our service? If your timeout was 1 second, your
success rate dropped to 95% during this period of time if you used
round robin. Your success rate only drops to 99.9% if you used EWMA.
99% if you used queue-depth as your metric.

We setup a load balancing simulation with response latencies from some
collected ping data. The median was 167ms with a stddev of 5ms and a
max of 200ms.

10 good clients and 1 client that slows to 2 seconds in the middle of
the test run and then recovers back to normal. This acts similar to a
bad GC pause in a backend. No timeouts enabled and no retries.

[TODO: find a way to do this automatically]
I hand-converted those quantiles into the following file (rr_latencies.txt):

Please note that the file uses tabs.

percentile         queue       ewma rr
"90"               184         183  187
"95"               189         188  203
"97"               193         191  1673
"98"               197         194  1683
"99"               205         199  1714
"99.9"             1723        211  1862
"99.99"            1825        1780 1977
"99.999"           1907        1819 1994

Here's how we generate the png of response times.

rr_latencies <- read.table("rr_latencies.txt", header=TRUE)
# turn this into a new dataframe for my own sanity. read.table is not being helpful
df <- data.frame(percentile=rr_latencies$percentile, ewma=rr_latencies$ewma, queue=rr_latencies$queue, roundrobin=rr_latencies$rr)
# builds an x-axis along the number of 9s we care about.
# the graph looks nuts along the automatically inferred scale so we'll force a continuous scale.
df$nines <- abs(log10(1.0 - df$percentile / 100))

png("rr_latencies.png", width=960, height=960)
theme_set(theme_bw(base_size = 27) + theme(legend.key.size=unit(35, "pt")))
ggplot(data=df, aes(x=nines, y=roundrobin)) +
       scale_x_continuous(breaks=df$nines,
                     labels=c("90", "95", "97", "98", "99", "99.9", "99.99", "99.999")) +
       geom_point(aes(y=roundrobin, col="Round Robin")) +
       geom_line(aes(y=roundrobin, col="Round Robin")) +
       geom_point(aes(y=ewma, col="EWMA")) +
       geom_line(aes(y=ewma, col="EWMA")) +
       geom_point(aes(y=queue, col="Least Loaded")) +
       geom_line(aes(y=queue, col="Least Loaded")) +
       labs(title="Latency by Load Balancer", x="Percentile", y="Latency (ms)", color="Balancers")
dev.off()
