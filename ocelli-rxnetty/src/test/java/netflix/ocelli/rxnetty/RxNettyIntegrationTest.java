package netflix.ocelli.rxnetty;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.reactivex.netty.RxNetty;
import io.reactivex.netty.protocol.http.client.HttpClientRequest;
import io.reactivex.netty.protocol.http.client.HttpClientResponse;
import io.reactivex.netty.protocol.http.server.HttpServer;
import io.reactivex.netty.protocol.http.server.HttpServerRequest;
import io.reactivex.netty.protocol.http.server.HttpServerResponse;
import io.reactivex.netty.protocol.http.server.RequestHandler;

import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import netflix.ocelli.Host;
import netflix.ocelli.Instance;
import netflix.ocelli.InstanceCollector;
import netflix.ocelli.InstanceQuarantiner;
import netflix.ocelli.InstanceSubject;
import netflix.ocelli.functions.Metrics;
import netflix.ocelli.loadbalancer.RoundRobinLoadBalancer;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import rx.Observable;
import rx.functions.Func1;

/**
 * @author Nitesh Kant
 */
public class RxNettyIntegrationTest {

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
            httpServer.shutdown();
        }
    }

    @Test
    public void testSimple() throws Exception {
        final InstanceSubject<Host> instances = InstanceSubject.create();

        final RoundRobinLoadBalancer<HttpClientHolder<ByteBuf, ByteBuf>> lb = RoundRobinLoadBalancer.create();
        
        instances
            .map(Instance.transform(new Func1<Host, HttpClientHolder<ByteBuf, ByteBuf>>() {
                @Override
                public HttpClientHolder<ByteBuf, ByteBuf> call(Host host) {
                    return new HttpClientHolder<ByteBuf, ByteBuf>(
                            RxNetty.createHttpClient(host.getHostName(), host.getPort()),
                            Metrics.memoize(10L));
                }
            }))
            // Quarantine logic
            .flatMap(InstanceQuarantiner.<HttpClientHolder<ByteBuf, ByteBuf>>create(HttpClientHolder.<ByteBuf, ByteBuf>factory()))
            // Aggregate into a List
            .compose(InstanceCollector.<HttpClientHolder<ByteBuf, ByteBuf>>create())
            // Forward to the load balancer
            .subscribe(lb);

        Host host = new Host("127.0.0.1", httpServer.getServerPort());
        instances.add(host);

        // Execute a single request
        HttpClientResponse<ByteBuf> response = lb
                .toObservable()
                .flatMap(new Func1<HttpClientHolder<ByteBuf, ByteBuf>, Observable<HttpClientResponse<ByteBuf>>>() {
                    @Override
                    public Observable<HttpClientResponse<ByteBuf>> call(HttpClientHolder<ByteBuf, ByteBuf> t1) {
                        return t1
                                .getClient()
                                .submit(HttpClientRequest.createGet("/"));
                    }
                })
                .delaySubscription(2, TimeUnit.SECONDS)
                .toBlocking()
                .toFuture()
                .get(3, TimeUnit.SECONDS);

        // Force failure
        lb.next().fail();
        
        // Execute a single request
        try {
            response = lb
                    .toObservable()
                    .flatMap(new Func1<HttpClientHolder<ByteBuf, ByteBuf>, Observable<HttpClientResponse<ByteBuf>>>() {
                        @Override
                        public Observable<HttpClientResponse<ByteBuf>> call(HttpClientHolder<ByteBuf, ByteBuf> t1) {
                            return t1
                                    .getClient()
                                    .submit(HttpClientRequest.createGet("/"));
                        }
                    })
                    .toBlocking()
                    .toFuture()
                    .get(1, TimeUnit.SECONDS);
        }
        catch (ExecutionException e) {
            Assert.assertSame(e.getCause().getClass(), NoSuchElementException.class);
        }

        TimeUnit.SECONDS.sleep(2);
        
        // Execute a single request
        response = lb
                .toObservable()
                .flatMap(new Func1<HttpClientHolder<ByteBuf, ByteBuf>, Observable<HttpClientResponse<ByteBuf>>>() {
                    @Override
                    public Observable<HttpClientResponse<ByteBuf>> call(HttpClientHolder<ByteBuf, ByteBuf> t1) {
                        return t1
                                .getClient()
                                .submit(HttpClientRequest.createGet("/"));
                    }
                })
                .toBlocking()
                .toFuture()
                .get(2, TimeUnit.SECONDS);

        Assert.assertEquals("Unexpected response status.", HttpResponseStatus.OK, response.getStatus());
        
        instances.remove(host);
        
        try {
            response = lb
                    .toObservable()
                    .flatMap(new Func1<HttpClientHolder<ByteBuf, ByteBuf>, Observable<HttpClientResponse<ByteBuf>>>() {
                        @Override
                        public Observable<HttpClientResponse<ByteBuf>> call(HttpClientHolder<ByteBuf, ByteBuf> t1) {
                            return t1
                                    .getClient()
                                    .submit(HttpClientRequest.createGet("/"));
                        }
                    })
                    .toBlocking()
                    .toFuture()
                    .get(1, TimeUnit.SECONDS);
        }
        catch (ExecutionException e) {
            Assert.assertSame(e.getCause().getClass(), NoSuchElementException.class);
        }

    }
    
//    @Test
//    public void testZoneFallback() throws Exception {
//        Observable<Instance<Host>> clientSource = Observable
//                .<Instance<Host>>just(CloseableInstance.from(new Host("127.0.0.1", httpServer.getServerPort())))
//                ;
//
//        Executor<HttpClientRequest<ByteBuf>, HttpClientResponse<ByteBuf>> executor = ExecutionStrategies.newHttpClient(clientSource).build();
//        
//        // Execute a single request
//        HttpClientResponse<ByteBuf> response = executor
//                .call(HttpClientRequest.createGet("/"))
//                .toBlocking()
//                .toFuture()
//                .get(1, TimeUnit.MINUTES);
//
//        Assert.assertEquals("Unexpected response status.", HttpResponseStatus.OK, response.getStatus());
//    }
}
