**PA1 \- Report**

**Results**

Below are the final results for our project with 2, 4, and 8 nodes.

**2 Nodes** (./run-cluster.sh 1 1):

node0 median 1158637 op/s

**total 1,158,637 op/s**

**4 Nodes** (./run-cluster.sh 2 2\)**:**

node0 median 1395847 op/s

node1 median 1395726 op/s

**total 2,791,573 op/s**

**8 Nodes** (./run-cluster.sh 4 4):

node0 median 1394158 op/s

node1 median 1391920 op/s

node2 median 1390298 op/s

node3 median 1407026 op/s

**total 5,583,403 op/s**

As for CPU utilization, we had an average of 87-92% usage on client nodes and 80-85% usage on server nodes. For memory, our servers used 4 out of 64 GB of available RAM, while clients used less than 1 GB. Our server nodes were sending out around 140,000 KB/s and receiving 32,000KB/s. So there was some room for improvement on hardware utilization, which could have resulted in more speedups. 

As for scaling, our solution scaled decently well with an increase in nodes. If we look at the max ops/s after the program had time to preallocate entries, we saw that each server averaged around \~1 \- 1.2 million ops/s when there were either 2, 4, or 8 nodes, so the performance scales decently with the number of nodes.

## **Design**

Single workload executes for a single server and client. This is considered a baseline to build upon. It resulted in 10,720 ops/s, where node1 had no load in that run.

Large map\[string\]string protected by a single sync.Mutex, which blocks the operations per‐request KV operation, is effectively single-server, meaning saturation with heavy lock contention and per-RPC overhead. Also, a single mutex is locking disjoint entities viz. Stats and the map access, and modifications. These entities have different access patterns and behavior, namely map need not be completely locked for concurrent reads. This led to servers working sequentially rather than in parallel.

We started with optimizing the server operations to maintain the access and read-write pattern to ensure the linearizability. The first optimization is that we separated the mutexes that accessed stats and data maps, also changing the data mutex to RWMutex. Later, we sharded this map based on keys and preallocated the shard with 100,000 entries. This increases the startup time, but subsequent operations are faster and consistent. We then later changed the stats parameter to an atomic operation for efficiency without breaking the behaviour. Also, we deferred the server-side print stats as these were the slowest when we profiled the KVS service.

Alternatively, we looked into using sync.Map as it was designed for faster concurrent reads and rare writes, as this looked the most suitable for our project. The reason we decided not to proceed with this design was that when the Golang gc expanded the structure, it slowed down the total operations by a lot.

With this implementation in place, the server nodes' CPU and memory utilization did not exceed 50-55%. This means we can run two servers on a single node for different ports. Effectively, we are running four server processes on 2 server nodes.

The next bottleneck we discovered was that the network bandwidth and CPU utilization were less than 40%. To address this issue, two options were considered: a) To batch multiple requests to improve network bandwidth utilization, and b) A Minimum of four threads per available core were launched on each server.

In this strategy, the clients accumulate GETs and send as one batch; flush on PUT or batch full; send final flush at loop end. The server handles BatchGet to process arrays in one RPC and to reuse locks per shard efficiently. The gain we get here is over network latency, RPC serialization, context switching, and lock acquisitions across many keys. The server processes many keys per shard under a single lock hold, dramatically improving CPU efficiency and cache locality.

A key consideration here is to make this design scalable with multiple servers. This required creating a batch for each server to ensure the same key is always sent to the same server.

Next up was increasing workload generation. The skeleton code only defined one workload per client. We tried to break up the one continuous workload into dozens of workloads for each client. This requires figuring out the number of threads at run time, then creating a few workloads per thread. Each workload can then be started asynchronously using go routines. Using these, we can also test the optimal number of workloads per thread. 

Increased batch size to 16 from 8, and increased per-host clients to runtime.NumCPU() \* 8 from runtime.NumCPU() \* 4

Larger batches further reduced costs; just enough clients to saturate CPU/network without excessive context switching; batch size large enough to reduce overhead cost.

In order to get the skeleton code to support more than one server, we began by sending each client's requests to their own individual server. This certainly helped increase output, but it technically wasn’t correct since each client would send all requests to their own server with no regard for other servers or what key they were requesting. But for the time being, this helped increase the distribution to different servers.

After client output was increased, we decided to come back to sharding to test it more optimally. We first increased shards to 128 and lengthened server startup sleep to 10 seconds, and let shard maps allocate/initialize. More shards reduced residual lock contention and improved cache locality under high parallelism. Extending startup time ensured servers were fully initialized (avoiding cold misses/alloc stalls) before the client blast. With high concurrency, too few shards become hot; too many shards cost memory & cache pressure. 128 was a good balance for the hardware. Proper warmup eliminates transient slow starts.

Another strategy we tried but didn’t carry out was a server side key hash map. Every time a key was sent to a server, it had to be put through a hash function to determine the correct shard. This strategy was to reduce the cost of hashing by keeping a map that stores keys with their shard locations. The first time a key was referenced, it would be hashed and stored in the map for future use. This ended up resulting in a large slowdown, taking ops/s from 2.8 million to just 800,000. This was caused by the extra initial cost to create and store these new hashes, causing the program to take much longer to ramp up to speed. If we increase the client duration to 60 seconds, it takes the ops/s to 2 million from 800,00, with longer duration seeing speeds up to 2.6 million. 

Instead of relying on a mutex to accumulate the number of operations, an atomic add was put in place. As it proved to be a better performance model. This could enqueue add operations one at a time in the put case, and for get, we could add an entire batch of gets at once. Similarly the lastPrint was also converted into int64 representation to ease the calculations.

**Reproducibility**

**Software dependencies** 

The libraries used for this project come from standard go libraries like, hash, sync.

So no additional setup is required.

**Configuration parameters**

The default CLI parameters from the skeleton code were used, but some optional parameters were added such as startup allocations, number of local shards in the server's code. On the other client side, the number of workloads and batch sizes for request. The only changes made to the **run-cluster.sh** were to launch multiple servers per server node. 

**Hardware requirements**

For our testing, we tested with 2 \- 8 nodes. Each node had 64 GB of RAM and 16 cores.

No other


**Steps to set up the experiment:**

**Clone our repo on the shared NFS and build**

Always work under `/mnt/nfs` so binaries and logs are visible to every node.

**1\. Open a terminal on node0 and run:**

1. cd /mnt/nfs  
2. mkdir \-p \<your-username\>  
3. cd /mnt/nfs/\<your-username\>

\# Use your deploy key or agent-forwarded GitHub key

1. git clone git@github.com:Dashree/cs6450-labs.git  
2. cd cs6450-labs  
3. git checkout pa1-turnin   
4. Run the command   
   ./run-cluster.sh 2 2 "-num-shards 128 \-alloc 1000000" "-thrds-per-host 32  \-batch-size 8"

Note:- this is for 2 servers 2 clients 

**2\. From `/mnt/nfs/<your-username>/cs6450-labs` run:**

\# Auto-split nodes into servers/clients:

./run-cluster.sh

**3\. Logs & Results**

The latest run logs are stored at:

/mnt/nfs/\<your-username\>/cs6450-labs/logs/latest

**Reflection**

An important takeaway from this assignment was to recognize where bottlenecks are early. In our early testing, we made improvements to our server, such as sharding and RWmutex, but the improvements were negligible. This was because the clients weren’t sending enough workload to fully stress the server to where there would be high volume collisions. These server-side optimizations proved to be useful, however, only after the clients were improved. Another important takeaway was that strategies often just needed slight changes to parameters, sometimes in unanticipated ways. An example of this was with batching; we first made an assumption that larger batch sizes would reduce the number of requests and result in the best improvement. But after some testing, we found that batch sizes of 8 were actually optimal, as it may have reduced packet sizes and reduced sudden loads on servers. 

Some ideas for further improvements would be caching frequently used keys on both the server and client sides. Since we were using ​​YCSB-B, there were some keys with a much higher usage rate. Making those keys readily available on the server would reduce the time to read from those busy keys. On the client side, a cache could have also helped, but it would also require testing to see if it would be optimal on frequently used keys or less frequently used keys, since frequently used keys also had a lot of writes. 

**Individual contributions**

In an attempt to keep each team member acquainted with different strategies, we did duplicate work. Sumaya, Jacob, and Fariba all did batching, sharding, and optimizations. Shreeda worked on combining our work into one solution, did profiling and optimization strategies, such as tweaking variables like thread counts, and added command-line arguments. Jacob worked on increasing workloads and debugging. We all worked in conjunction to set up experiments, test results, and write the final document.

