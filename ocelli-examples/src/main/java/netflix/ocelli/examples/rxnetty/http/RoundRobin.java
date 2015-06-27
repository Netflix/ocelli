package netflix.ocelli.examples.rxnetty.http;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.reactivex.netty.protocol.http.client.HttpClient;
import io.reactivex.netty.protocol.http.client.events.HttpClientEventsListener;
import io.reactivex.netty.protocol.http.server.HttpServer;
import netflix.ocelli.Instance;
import netflix.ocelli.rxnetty.protocol.http.HttpLoadBalancer;
import rx.Observable;

import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.charset.Charset;
import java.util.concurrent.TimeUnit;

import static io.netty.handler.codec.http.HttpResponseStatus.*;

public final class RoundRobin {

    private RoundRobin() {
    }

    public static void main(String[] args) {

        Observable<Instance<SocketAddress>> hosts = Observable.just(startServer(OK), startServer(SERVICE_UNAVAILABLE),
                                                                    new InetSocketAddress(0))
                                                              .map(socketAddr -> new Instance<SocketAddress>() {
                                                                  @Override
                                                                  public Observable<Void> getLifecycle() {
                                                                      return Observable.never();
                                                                  }

                                                                  @Override
                                                                  public SocketAddress getValue() {
                                                                      return socketAddr;
                                                                  }
                                                              });

        HttpClient.newClient(HttpLoadBalancer.<ByteBuf, ByteBuf>roundRobin(hosts, failureListener -> {
                                return new HttpClientEventsListener() {
                                    @Override
                                    public void onResponseHeadersReceived(int responseCode) {
                                        if (responseCode == 503) {
                                            // When throttled, quarantine.
                                            failureListener.quarantine(1, TimeUnit.MINUTES);
                                        }
                                    }

                                    @Override
                                    public void onConnectFailed(long duration, TimeUnit timeUnit, Throwable throwable) {
                                        // Connect failed, remove
                                        failureListener.remove();
                                    }
                                };
                             }).toConnectionProvider())
                  .createGet("/hello")
                  .doOnNext(System.out::println)
                  .flatMap(resp -> {
                      if (resp.getStatus().code() != 200) {
                          return Observable.error(new InvalidResponseException());
                      }
                      return resp.getContent();
                  })
                  .retry((integer, throwable) -> throwable instanceof SocketException
                                                 || throwable instanceof ConnectException
                                                 || throwable instanceof InvalidResponseException)
                  .repeat(10)
                  .toBlocking()
                  .forEach(bb -> bb.toString(Charset.defaultCharset()));
    }

    protected static SocketAddress startServer(HttpResponseStatus cannedStatus) {
        return HttpServer.newServer()
                         .start((request, response) -> {
                             return response.addHeader("X-Instance", response.unsafeNettyChannel().localAddress())
                                            .setStatus(cannedStatus);
                         })
                         .getServerAddress();
    }

    private static class InvalidResponseException extends RuntimeException {

        private static final long serialVersionUID = -712946630951320233L;

        public InvalidResponseException() {
        }
    }
}
