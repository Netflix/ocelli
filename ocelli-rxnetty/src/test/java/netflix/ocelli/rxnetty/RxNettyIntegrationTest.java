package netflix.ocelli.rxnetty;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.reactivex.netty.RxNetty;
import io.reactivex.netty.protocol.http.client.HttpClient;
import io.reactivex.netty.protocol.http.client.HttpClientRequest;
import io.reactivex.netty.protocol.http.client.HttpClientResponse;
import io.reactivex.netty.protocol.http.server.HttpServer;
import io.reactivex.netty.protocol.http.server.HttpServerRequest;
import io.reactivex.netty.protocol.http.server.HttpServerResponse;
import io.reactivex.netty.protocol.http.server.RequestHandler;

import java.util.concurrent.TimeUnit;

import netflix.ocelli.Host;
import netflix.ocelli.LoadBalancer;
import netflix.ocelli.LoadBalancers;
import netflix.ocelli.MembershipEvent;
import netflix.ocelli.selectors.RandomWeightedSelector;
import netflix.ocelli.selectors.weighting.LinearWeightingStrategy;

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
        final HttpClientPool<ByteBuf, ByteBuf> clientPool = HttpClientPool.newPool();
        Observable<HttpClient<ByteBuf, ByteBuf>> clientSource = Observable.just(new Host("127.0.0.1", httpServer.getServerPort()))
                                                                          .map(new Func1<Host, HttpClient<ByteBuf, ByteBuf>>() {
                                                                              @Override
                                                                              public HttpClient<ByteBuf, ByteBuf> call(
                                                                                      Host host) {
                                                                                  return clientPool.getClientForHost(host);
                                                                              }
                                                                          });

        final LoadBalancer<HttpClientHolder<ByteBuf, ByteBuf>> lb =
                LoadBalancers
                    .newBuilder(clientSource.map(new Func1<HttpClient<ByteBuf, ByteBuf>, MembershipEvent<HttpClientHolder<ByteBuf, ByteBuf>>>() {
                            @Override
                            public MembershipEvent<HttpClientHolder<ByteBuf, ByteBuf>> call(
                                    HttpClient<ByteBuf, ByteBuf> client) {
                                return new MembershipEvent<HttpClientHolder<ByteBuf, ByteBuf>>(
                                        MembershipEvent.EventType.ADD,
                                        new HttpClientHolder<ByteBuf, ByteBuf>(
                                                client));
                            }
                        }))
                        .withSelectionStrategy(
                            new RandomWeightedSelector<HttpClientHolder<ByteBuf, ByteBuf>>(
                                new LinearWeightingStrategy<HttpClientHolder<ByteBuf, ByteBuf>>(
                                    new RxNettyPendingRequests<ByteBuf, ByteBuf>())))
                        .withFailureDetector(new RxNettyFailureDetector<ByteBuf, ByteBuf>())
                        .build();

        HttpClientResponse<ByteBuf> response = lb.choose().flatMap(
                new Func1<HttpClientHolder<ByteBuf, ByteBuf>, Observable<HttpClientResponse<ByteBuf>>>() {
                    @Override
                    public Observable<HttpClientResponse<ByteBuf>> call(HttpClientHolder<ByteBuf, ByteBuf> holder) {
                        return holder.getClient().submit(HttpClientRequest.createGet("/"))
                                     .map(new Func1<HttpClientResponse<ByteBuf>, HttpClientResponse<ByteBuf>>() {
                                         @Override
                                         public HttpClientResponse<ByteBuf> call(HttpClientResponse<ByteBuf> response) {
                                             response.ignoreContent();
                                             return response;
                                         }
                                     });
                    }
                }).toBlocking().toFuture().get(1, TimeUnit.MINUTES);

        Assert.assertEquals("Unexpected response status.", HttpResponseStatus.OK, response.getStatus());
    }
}
