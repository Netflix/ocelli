package netflix.ocelli.rxnetty;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.reactivex.netty.RxNetty;
import io.reactivex.netty.pipeline.PipelineConfigurators;
import io.reactivex.netty.protocol.http.server.HttpServer;
import io.reactivex.netty.protocol.http.server.HttpServerRequest;
import io.reactivex.netty.protocol.http.server.HttpServerResponse;
import io.reactivex.netty.protocol.http.server.RequestHandler;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

import netflix.ocelli.Host;

import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;
import rx.functions.Func1;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

public class NettyServerFarmResource extends ExternalResource {
    private static final Logger LOG = LoggerFactory.getLogger(NettyServerFarmResource.class);
        
    private final int count;
    private final HashMap<Host, HttpServer<ByteBuf, ByteBuf>> servers = Maps.newHashMap();
    private final List<ImmutableMap<String, String>> attributes;
    
    public NettyServerFarmResource(List<ImmutableMap<String, String>> arrayList) {
        this.count = arrayList.size();
        this.attributes = arrayList;
    }
    
    public NettyServerFarmResource(int count) {
        this.count = count;
        this.attributes = null;
    }
    
    @Override
    protected void before() throws Throwable {
        for (int i = 0; i < count; i++) {
            HttpServer<ByteBuf, ByteBuf> server = createServer();
            server.start();
            
            LOG.info("Starting server: localhost:" + server.getServerPort());
            servers.put(
                new Host("localhost", server.getServerPort(), attributes != null ? attributes.get(i) : null),
                server);
        }
    }

    @Override
    protected void after() {
        for (HttpServer<ByteBuf, ByteBuf> server : servers.values()) {
            try {
                server.shutdown();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
    
    public HttpServer<ByteBuf, ByteBuf> createServer() {
        return RxNetty
                .newHttpServerBuilder(0, new RequestHandler<ByteBuf, ByteBuf>() {
                    @Override
                    public Observable<Void> handle(HttpServerRequest<ByteBuf> request, final HttpServerResponse<ByteBuf> response) {
//                        LOG.info("Request: " + request.getUri());
                        
                        QueryParameters params = QueryParameters.from(request.getQueryParameters());
                        Integer delay = params.firstAsInt("delay");
                        Integer code  = params.firstAsInt("code");
                        
                        // Optional response code
                        if (code != null) {
                            response.setStatus(HttpResponseStatus.valueOf(code));
                        }
                        
                        // Message body
                        response.writeString("Welcome!!");
                        
                        // Optional delay
                        if (delay != null) {
//                            LOG.info("Delaying " + delay);
                            return Observable
                                .timer(delay, TimeUnit.MILLISECONDS)
                                .flatMap(new Func1<Long, Observable<Void>>() {
                                    @Override
                                    public Observable<Void> call(Long t1) {
                                        return response.close(true);
                                    }
                                });
                        }
                        return response.close(true);
                    }
                })
                .pipelineConfigurator(PipelineConfigurators.<ByteBuf, ByteBuf>httpServerConfigurator())
                .build();
    }
    
    public Observable<Host> hosts() {
        return Observable.from(servers.keySet());
    }
}
