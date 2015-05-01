package netflix.ocelli.rxnetty;

import io.netty.buffer.ByteBuf;
import io.reactivex.netty.RxNetty;
import io.reactivex.netty.protocol.http.client.HttpClient;
import io.reactivex.netty.protocol.http.client.HttpClientRequest;
import io.reactivex.netty.protocol.http.client.HttpClientResponse;
import io.reactivex.netty.protocol.http.server.HttpServer;
import io.reactivex.netty.protocol.http.server.HttpServerRequest;
import io.reactivex.netty.protocol.http.server.HttpServerResponse;
import io.reactivex.netty.protocol.http.server.RequestHandler;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import netflix.ocelli.Host;
import netflix.ocelli.Instance;
import netflix.ocelli.InstanceCollector;
import netflix.ocelli.InstanceSubject;
import netflix.ocelli.LoadBalancer;
import netflix.ocelli.functions.Delays;
import netflix.ocelli.functions.Metrics;
import netflix.ocelli.loadbalancer.RoundRobinLoadBalancer;
import netflix.ocelli.util.RxUtil;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import rx.Observable;
import rx.functions.Func1;

public class ServerPoolTest {

    private HttpServer<ByteBuf, ByteBuf> httpServer;

    @Before
    public void setUp() throws Exception {
        httpServer = RxNetty.createHttpServer(0, new RequestHandler<ByteBuf, ByteBuf>() {
            @Override
            public Observable<Void> handle(HttpServerRequest<ByteBuf> request, HttpServerResponse<ByteBuf> response) {
                return response.close();
            }
        });
        httpServer.start();
    }

    @After
    public void tearDown() throws Exception {
        if (null != httpServer) {
            try {
                httpServer.shutdown();
            }
            catch (Exception e) {
                
            }
        }
    }

    @Test
    public void testSimple() throws Exception {
        final InstanceSubject<Host> instances = InstanceSubject.create();

        final Map<Host, HttpInstanceImpl> lookup = new HashMap<Host, HttpInstanceImpl>();
        
        final LoadBalancer<HttpClient<ByteBuf, ByteBuf>> lb = LoadBalancer.fromSource(
                instances
                    .map(new Func1<Instance<Host>, Instance<HttpInstanceImpl>>() {
                        @Override
                        public Instance<HttpInstanceImpl> call(Instance<Host> t1) {
                            return Instance.create(
                                    new HttpInstanceImpl(t1.getValue(), Metrics.quantile(0.95), t1.getLifecycle()), 
                                    t1.getLifecycle());
                        }
                    })
                    .doOnNext(InstanceCollector.toMap(lookup, HttpInstanceImpl.byHost())))
                    .withQuarantiner(HttpInstanceImpl.connector(), Delays.immediate())
                    // Convert from HttpServer to an HttpClient while managing event subscriptions
                    .convertTo(HttpInstanceImpl.toClient())
                    .build(RoundRobinLoadBalancer.<HttpClient<ByteBuf, ByteBuf>>create());
        
        // Case 1: Simple add
        Host host = new Host("127.0.0.1", httpServer.getServerPort());
        instances.add(host);
        
        // Case 2: Attempt an operation
        HttpClientResponse<ByteBuf> resp = lb.toBlocking().first()
                .submit(HttpClientRequest.createGet("/"))
                .timeout(1, TimeUnit.SECONDS)
                .toBlocking()
                .first();

        Assert.assertEquals(200, resp.getStatus().code());
        
        HttpInstanceImpl s1 = lookup.get(host);
        Assert.assertNotNull(s1);
        Assert.assertEquals(1, s1.getMetricListener().getIncarnationCount());

        // Case 3: Remove the server
        instances.remove(host);
        try {
            lb.toBlocking().first();
            Assert.fail("Should have failed with no element");
        }
        catch (NoSuchElementException e) {
        }
        
        // Case 4: Add a bad host and confirm retry counts
        Host badHost = new Host("127.0.0.2.1", 0);
        instances.add(badHost);
        
        AtomicInteger attemptCount = new AtomicInteger();
        
        try {
            resp = lb
                    .doOnNext(RxUtil.increment(attemptCount))
                    .concatMap(new Func1<HttpClient<ByteBuf, ByteBuf>, Observable<HttpClientResponse<ByteBuf>>>() {
                        @Override
                        public Observable<HttpClientResponse<ByteBuf>> call(HttpClient<ByteBuf, ByteBuf> client) {
                            return client.submit(HttpClientRequest.createGet("/"));
                        }
                    })
                    .retry(1)
                    .timeout(1, TimeUnit.SECONDS)
                    .toBlocking()
                    .first();
            Assert.fail("Should have failed with connect timeout exception");
        }
        catch (Exception e) {
            Assert.assertEquals(2, attemptCount.get());
        }
        
        HttpInstanceImpl s2 = lookup.get(badHost);
        Assert.assertEquals(3, s2.getMetricListener().getIncarnationCount());
//        httpServer.start();
        
        
//      .map(Instance.transform(new Func1<Host, HttpClientHolder<ByteBuf, ByteBuf>>() {
//      @Override
//      public HttpClientHolder<ByteBuf, ByteBuf> call(Host host) {
//          return new HttpClientHolder<ByteBuf, ByteBuf>(
//                  RxNetty.createHttpClient(host.getHostName(), host.getPort()),
//                  Metrics.memoize(10L));
//      }
//  }))

//
//        // Execute a single request
//        HttpClientResponse<ByteBuf> response = lb
//                .toObservable()
//                .flatMap(new Func1<HttpClientHolder<ByteBuf, ByteBuf>, Observable<HttpClientResponse<ByteBuf>>>() {
//                    @Override
//                    public Observable<HttpClientResponse<ByteBuf>> call(HttpClientHolder<ByteBuf, ByteBuf> t1) {
//                        return t1
//                                .getClient()
//                                .submit(HttpClientRequest.createGet("/"));
//                    }
//                })
//                .delaySubscription(2, TimeUnit.SECONDS)
//                .toBlocking()
//                .toFuture()
//                .get(3, TimeUnit.SECONDS);
//
//        // Force failure
//        lb.next().fail();
//        
//        // Execute a single request
//        try {
//            response = lb
//                    .toObservable()
//                    .flatMap(new Func1<HttpClientHolder<ByteBuf, ByteBuf>, Observable<HttpClientResponse<ByteBuf>>>() {
//                        @Override
//                        public Observable<HttpClientResponse<ByteBuf>> call(HttpClientHolder<ByteBuf, ByteBuf> t1) {
//                            return t1
//                                    .getClient()
//                                    .submit(HttpClientRequest.createGet("/"));
//                        }
//                    })
//                    .toBlocking()
//                    .toFuture()
//                    .get(1, TimeUnit.SECONDS);
//        }
//        catch (ExecutionException e) {
//            Assert.assertSame(e.getCause().getClass(), NoSuchElementException.class);
//        }
//
//        TimeUnit.SECONDS.sleep(2);
//        
//        // Execute a single request
//        response = lb
//                .toObservable()
//                .flatMap(new Func1<HttpClientHolder<ByteBuf, ByteBuf>, Observable<HttpClientResponse<ByteBuf>>>() {
//                    @Override
//                    public Observable<HttpClientResponse<ByteBuf>> call(HttpClientHolder<ByteBuf, ByteBuf> t1) {
//                        return t1
//                                .getClient()
//                                .submit(HttpClientRequest.createGet("/"));
//                    }
//                })
//                .toBlocking()
//                .toFuture()
//                .get(2, TimeUnit.SECONDS);
//
//        Assert.assertEquals("Unexpected response status.", HttpResponseStatus.OK, response.getStatus());
//        
//        instances.remove(host);
//        
//        try {
//            response = lb
//                    .toObservable()
//                    .flatMap(new Func1<HttpClientHolder<ByteBuf, ByteBuf>, Observable<HttpClientResponse<ByteBuf>>>() {
//                        @Override
//                        public Observable<HttpClientResponse<ByteBuf>> call(HttpClientHolder<ByteBuf, ByteBuf> t1) {
//                            return t1
//                                    .getClient()
//                                    .submit(HttpClientRequest.createGet("/"));
//                        }
//                    })
//                    .toBlocking()
//                    .toFuture()
//                    .get(1, TimeUnit.SECONDS);
//        }
//        catch (ExecutionException e) {
//            Assert.assertSame(e.getCause().getClass(), NoSuchElementException.class);
//        }

    }
}
