# Ocelli : Generic client load balancer

Ocelli is an extremely simple but powerful implementation of a client side load balancer.  Ocelli takes the reactive approach to provide a composable load balancer through which any number of load balancing and failover algorithms may be implemented.  The SPI for Ocelli is as simple as a subscribing to a load balancer Observable that emits a single client.  Executing an operation on the client is simply a mapping for the client to a response.  Retries are implemented using standard RxJava functions such as retry() and onErrorResumeNext().  

```java
Observable<Client> loadBalancer = ...;

loadBalancer
  .choose()
  .concatMap((client) -> client.doSomethingRetrunsAnObservableResponse())
  .retry(2) 
  .subscribe();
```

