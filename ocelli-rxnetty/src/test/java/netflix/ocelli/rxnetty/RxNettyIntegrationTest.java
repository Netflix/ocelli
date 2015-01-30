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

import java.util.concurrent.TimeUnit;

import netflix.ocelli.FailureDetectingClientLifecycleFactory;
import netflix.ocelli.Host;
import netflix.ocelli.HostToClient;
import netflix.ocelli.HostToClientCollector;
import netflix.ocelli.LoadBalancer;
import netflix.ocelli.MembershipEvent;
import netflix.ocelli.MembershipEvent.EventType;
import netflix.ocelli.PoolingHostToClientLifecycleFactory;
import netflix.ocelli.execute.SimpleExecutionStrategy;
import netflix.ocelli.functions.Delays;
import netflix.ocelli.functions.Metrics;
import netflix.ocelli.loadbalancer.ChoiceOfTwoLoadBalancer;
import netflix.ocelli.loadbalancer.RoundRobinLoadBalancer;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import rx.Observable;
import rx.functions.Action1;
import rx.subjects.PublishSubject;

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
        Observable<Host> clientSource = Observable
                .just(new Host("127.0.0.1", httpServer.getServerPort()));

        // Create a factory/pool of client objects
        PoolingHostToClientLifecycleFactory<Host, HttpClientHolder<ByteBuf, ByteBuf>> factory = 
            new PoolingHostToClientLifecycleFactory<Host, HttpClientHolder<ByteBuf, ByteBuf>>(
                new HostToClient<Host, HttpClientHolder<ByteBuf, ByteBuf>>() {
                    @Override
                    public HttpClientHolder<ByteBuf, ByteBuf> call(Host host) {
                        return new HttpClientHolder<ByteBuf, ByteBuf>(
                                RxNetty.createHttpClient(host.getHostName(), host.getPort()), 
                                Metrics.memoize(0L));
                    }
                }, 
                FailureDetectingClientLifecycleFactory.<HttpClientHolder<ByteBuf, ByteBuf>>builder()
                    .withQuarantineStrategy(Delays.fixed(1, TimeUnit.SECONDS))
                    .withFailureDetector(new RxNettyFailureDetector<ByteBuf, ByteBuf>())
                    .withClientShutdown(new Action1<HttpClientHolder<ByteBuf, ByteBuf>>() {
                        @Override
                        public void call(HttpClientHolder<ByteBuf, ByteBuf> t1) {
                            t1.getClient().shutdown();
                        }
                    })
                    .build());        
        
        // Create a single load balancer for ALL hosts
        final LoadBalancer<HttpClientHolder<ByteBuf, ByteBuf>> lb =
            RoundRobinLoadBalancer
                .create(clientSource
                    .map(MembershipEvent.<Host>toEvent(EventType.ADD))
                    .lift(HostToClientCollector.create(factory)));
        
        SimpleExecutionStrategy<HttpClientHolder<ByteBuf, ByteBuf>> executor = new SimpleExecutionStrategy<HttpClientHolder<ByteBuf, ByteBuf>>(lb);
        
        // Execute a single request
        HttpClientResponse<ByteBuf> response = executor
                .execute(Requests.from(HttpClientRequest.createGet("/")))
                .toBlocking()
                .toFuture()
                .get(1, TimeUnit.MINUTES);

        Assert.assertEquals("Unexpected response status.", HttpResponseStatus.OK, response.getStatus());
    }
}
