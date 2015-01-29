## What is Ocelli?

Ocelli is a client side reactive load balancer based on RxJava.

```java
Observable<Client> loadBalancer = ...;

loadBalancer
  .concatMap((client) -> client.doSomethingRetrunsAnObservableResponse())
  .retry(2) 
  .subscribe();
```


## Why Ocelli?

In biology Ocelli is a simple eye as opposed to a complex eye.  Ocelli the library is a simple load balancer implementation as opposed to very complex load balancers that do too many via magic or are too tightly coupled with the client transport.  Don't know how to pronounce Ocelli?  Goto http://www.merriam-webster.com/audio.php?file=ocellu02&word=ocelli

## How is Ocelli Reactive?

Ocelli exposes a very simple API to choose a client.  A new client is emitted for each subscription to the return Observable based on changes within the load balancer and optimization based on the load balancing algorithm.  Ocelli reacts to changes in the network topology either via server membership events or server failure detection.

## How is Ocelli Functional?

Ocelli is policy driven by delegating key strategies and metric sources via simple functions.  This approach simplifies the load balancer implementation so that it is decoupled from the speicific client SPI.  These functions are essentially the glue between the load balancer algorithm and the client SPI.

## Key design principles

### Metrics
Ocelli doesn't attempt to track metrics for client libraries.  That role is delegated to the client libraries since most libraries already perform that task.  Ocelli simply provides hooks (via functions) for libraries to provide the metric value.

### Retry
Retry friendly, not retry magic! Code that performs operations in multiple layers is easily susceptible to retry storms where each layer is unaware of retries in the next.  Ocelli makes retries possible by tracking state so that retries are efficient but does not force any particular retry policy.  Instead Ocelli let's the caller specify retry policieis via RxJava's built in retry operations such as retry(), retryWhen() and onErrorResumeNext().  

### Composability 
Ocelli is composable in that the load balancer can be combined with other functionality such as caching, retry and failover.  Ocelli delegates as much functionality to the RxJava operators to give users of the library full control.  In most cases specific policies may be specified in just one line of code.

### Light configuration
TODO

## Key features

### Pluggable load balancing strategy

### Pluggable metrics gathering

### Partitioning

Services are sometimes partitioned to distribute data, load or functionality.  Ocelli provides a partitioning mechanism whereby Servers can be members of 0 to many partitions.  The client can then choose to route traffic only to one partition and fall back to other partitions.  For example, partitioning can be used to implement a rack aware topology where all requests are first directed at the same rack as the client but can fail over to other racks.

### Round robin vs. Weighted Round Robin

### Pluggable weighting strategy

#### Idnetity

The weight provided by the function is used as the weight.  The highest value translates into a higher weight.

#### reverseMax

The weight is calculated as the difference between the max and the value.  The lowest value therefore translates into a higher 

### Testing

### Topologies

Topologies are similar to partitions but deal more with the changing position of the client host within a larger topology.  Topologies also don't deal with data or functional aspects of a service.  For example, to limit the number of hosts to which a client will connect the entire set of servers (including the client) are arranged into a ring and the client connects only to a subset of hosts within the ring using consistent hashing to ensure even distribution.  As servers come in an out of existence Ocelli will adjust its target set of hosts to meet the new topology.

### Rate limiter
TODO
