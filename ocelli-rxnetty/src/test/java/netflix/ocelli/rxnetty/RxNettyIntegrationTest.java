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

import netflix.ocelli.Host;
import netflix.ocelli.MembershipEvent;
import netflix.ocelli.MembershipEvent.EventType;
import netflix.ocelli.executor.Executor;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import rx.Observable;

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
        Observable<MembershipEvent<Host>> clientSource = Observable
                .just(new Host("127.0.0.1", httpServer.getServerPort()))
                .map(MembershipEvent.<Host>toEvent(EventType.ADD))
                ;

        
        Executor<HttpClientRequest<ByteBuf>, HttpClientResponse<ByteBuf>> executor = ExecutionStrategies.newHttpClient(clientSource).build();
        
        // Execute a single request
        HttpClientResponse<ByteBuf> response = executor
                .call(HttpClientRequest.createGet("/"))
                .toBlocking()
                .toFuture()
                .get(1, TimeUnit.MINUTES);

        Assert.assertEquals("Unexpected response status.", HttpResponseStatus.OK, response.getStatus());
    }
    
    @Test
    public void testZoneFallback() throws Exception {
        Observable<MembershipEvent<Host>> clientSource = Observable
                .just(new Host("127.0.0.1", httpServer.getServerPort()))
                .map(MembershipEvent.<Host>toEvent(EventType.ADD))
                ;

        Executor<HttpClientRequest<ByteBuf>, HttpClientResponse<ByteBuf>> executor = ExecutionStrategies.newHttpClient(clientSource).build();
        
        // Execute a single request
        HttpClientResponse<ByteBuf> response = executor
                .call(HttpClientRequest.createGet("/"))
                .toBlocking()
                .toFuture()
                .get(1, TimeUnit.MINUTES);

        Assert.assertEquals("Unexpected response status.", HttpResponseStatus.OK, response.getStatus());
    }
}
