package netflix.ocelli.examples.rxnetty.http;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.reactivex.netty.protocol.http.server.HttpServer;
import netflix.ocelli.Instance;
import rx.Observable;
import rx.functions.Func1;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.TimeUnit;

final class HttpExampleUtils {

    private HttpExampleUtils() {
    }

    protected static Observable<Instance<SocketAddress>> newHostStreamWithCannedLatencies(Long... latencies) {
        return Observable.from(latencies)
                         .map(latency -> {
                             return startServer(latency);
                         })
                         .map(new SockAddrToInstance());
    }

    protected static Observable<Instance<SocketAddress>> newHostStreamWithCannedStatus(
            HttpResponseStatus... cannedStatuses) {
        return Observable.from(cannedStatuses)
                         .map(cannedStatus -> {
                             if (null != cannedStatus) {
                                 return startServer(cannedStatus);
                             }
                             return new InetSocketAddress(0);
                         })
                         .map(new SockAddrToInstance());
    }

    protected static SocketAddress startServer(long latencyMillis) {
        return HttpServer.newServer()
                         .start((request, response) -> {
                             return Observable.timer(latencyMillis, TimeUnit.MILLISECONDS)
                                              .flatMap(aTick -> response.addHeader("X-Instance",
                                                                                   response.unsafeNettyChannel()
                                                                                           .localAddress())
                                                                        .setStatus(HttpResponseStatus.OK));
                         })
                         .getServerAddress();
    }

    protected static SocketAddress startServer(HttpResponseStatus cannedStatus) {
        return HttpServer.newServer()
                         .start((request, response) -> {
                             return response.addHeader("X-Instance", response.unsafeNettyChannel().localAddress())
                                            .setStatus(cannedStatus);
                         })
                         .getServerAddress();
    }

    protected static class InvalidResponseException extends RuntimeException {

        private static final long serialVersionUID = -712946630951320233L;

        public InvalidResponseException() {
        }
    }

    private static class SockAddrToInstance implements Func1<SocketAddress, Instance<SocketAddress>> {
        @Override
        public Instance<SocketAddress> call(SocketAddress socketAddr) {
            return new Instance<SocketAddress>() {

                @Override
                public Observable<Void> getLifecycle() {
                    return Observable.never();
                }

                @Override
                public SocketAddress getValue() {
                    return socketAddr;
                }
            };
        }
    }
}
