package netflix.ocelli.rxnetty;

import com.google.common.collect.Maps;
import io.netty.buffer.ByteBuf;
import io.reactivex.netty.RxNetty;
import io.reactivex.netty.pipeline.PipelineConfigurators;
import io.reactivex.netty.protocol.http.server.HttpServer;
import io.reactivex.netty.protocol.http.server.HttpServerRequest;
import io.reactivex.netty.protocol.http.server.HttpServerResponse;
import io.reactivex.netty.protocol.http.server.RequestHandler;
import netflix.ocelli.Host;
import netflix.ocelli.MembershipEvent;
import netflix.ocelli.MembershipEvent.EventType;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

import java.util.HashMap;

public class NettyServerFarmResource extends ExternalResource {
    private static final Logger LOG = LoggerFactory.getLogger(NettyServerFarmResource.class);
        
    private final int count;
    private final HashMap<Host, HttpServer<ByteBuf, ByteBuf>> servers = Maps.newHashMap();
    
    public NettyServerFarmResource(int count) {
        this.count = count;
    }
    
    @Override
    protected void before() throws Throwable {
        for (int i = 0; i < count; i++) {
            HttpServer<ByteBuf, ByteBuf> server = createServer();
            server.start();
            
            LOG.info("Starting server: localhost:" + server.getServerPort());
            servers.put(
                new Host("localhost", server.getServerPort()),
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
        return RxNetty.newHttpServerBuilder(0, new RequestHandler<ByteBuf, ByteBuf>() {
            @Override
            public Observable<Void> handle(HttpServerRequest<ByteBuf> request, final HttpServerResponse<ByteBuf> response) {
                response.writeString("Welcome!!");
                return response.close(false);
            }
        }).pipelineConfigurator(PipelineConfigurators.<ByteBuf, ByteBuf>httpServerConfigurator()).build();
    }
    
    public Observable<MembershipEvent<Host>> hostEvents() {
        return Observable.from(servers.keySet()).map(MembershipEvent.<Host>toEvent(EventType.ADD));
    }
}
