package netflix.ocelli.rxnetty;

import java.util.concurrent.TimeUnit;

import io.netty.buffer.ByteBuf;
import io.reactivex.netty.RxNetty;
import io.reactivex.netty.pipeline.PipelineConfigurators;
import io.reactivex.netty.protocol.http.server.HttpServer;
import io.reactivex.netty.protocol.http.server.HttpServerRequest;
import io.reactivex.netty.protocol.http.server.HttpServerResponse;
import io.reactivex.netty.protocol.http.server.RequestHandler;
import rx.Observable;

public class RxNettyServerFarm {
    private static int SERVER_COUNT = 50;
    
    public static void main(String[] args) throws InterruptedException {
        for (int i = 0; i < SERVER_COUNT; i++) {
            HttpServer<ByteBuf, ByteBuf> server = createServer(8080+i);
            server.start();
        }
        
        TimeUnit.HOURS.sleep(1);
    }

    public static HttpServer<ByteBuf, ByteBuf> createServer(int port) {
        HttpServer<ByteBuf, ByteBuf> server = RxNetty.newHttpServerBuilder(port, new RequestHandler<ByteBuf, ByteBuf>() {
            @Override
            public Observable<Void> handle(HttpServerRequest<ByteBuf> request, final HttpServerResponse<ByteBuf> response) {
                response.writeString("Welcome!!");
                return response.close(false);
            }
        }).pipelineConfigurator(PipelineConfigurators.<ByteBuf, ByteBuf>httpServerConfigurator()).build();

        return server;
    }
}
