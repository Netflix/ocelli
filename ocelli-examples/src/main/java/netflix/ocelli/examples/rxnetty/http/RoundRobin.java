package netflix.ocelli.examples.rxnetty.http;

import io.netty.buffer.ByteBuf;
import io.reactivex.netty.protocol.http.client.HttpClient;
import io.reactivex.netty.protocol.http.client.events.HttpClientEventsListener;
import netflix.ocelli.Instance;
import netflix.ocelli.examples.rxnetty.http.HttpExampleUtils.*;
import netflix.ocelli.rxnetty.protocol.http.HttpLoadBalancer;
import rx.Observable;

import java.net.ConnectException;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.charset.Charset;
import java.util.concurrent.TimeUnit;

import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static netflix.ocelli.examples.rxnetty.http.HttpExampleUtils.*;

public final class RoundRobin {

    private RoundRobin() {
    }

    public static void main(String[] args) {

        Observable<Instance<SocketAddress>> hosts = newHostStreamWithCannedStatus(OK, SERVICE_UNAVAILABLE,
                                                                                  null/*Unavailable socket address*/);

        HttpLoadBalancer<ByteBuf, ByteBuf> lb =
                HttpLoadBalancer.<ByteBuf, ByteBuf>roundRobin(hosts, failureListener -> {
                    return new HttpClientEventsListener() {
                        @Override
                        public void onResponseHeadersReceived(int responseCode, long duration, TimeUnit timeUnit) {
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
                });

        HttpClient.newClient(lb.toConnectionProvider())
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
}
