RxLoadBalancer
==============

RxLoadBalancer is an RxJava based software load balancer.

RxLoadBalancer provides an exteremely simple Rx API for getting a Client from the load balancer.  Simply call select() and get back an Observable that when subscribed to will emit a single Client based on the configured load balancer algorithms.  Operations are 'executed' as a concateMap or flatMap from the Client to the desired response.

```java
Observable<Client> loadBalancer = ...;

loadBalancer.select()
  .concatMap(new Func1<Client, Observable<Response>>() {
     Observable<Response> call(Client client) {
         return client.doSomethingThatReturnsAResponse();
     }
  })
  .subscribe();
```

Since RxLoadBalancer is Rx based it's very easy to add user defined retry policies.

```java
loadBalancer.select()
  .concateMap(someOperation)
  .retry(3)
  .subscribe();
```
