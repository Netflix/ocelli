package netflix.ocelli.examples.rxnetty.http;

import io.netty.buffer.ByteBuf;
import io.reactivex.netty.protocol.http.client.HttpClient;
import netflix.ocelli.Instance;
import netflix.ocelli.rxnetty.protocol.http.HttpLoadBalancer;
import netflix.ocelli.rxnetty.protocol.http.WeightedHttpClientListener;
import rx.Observable;

import java.net.ConnectException;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.charset.Charset;
import java.util.concurrent.TimeUnit;

import static netflix.ocelli.examples.rxnetty.http.HttpExampleUtils.*;

public final class ChoiceOfTwo {

    private ChoiceOfTwo() {
    }

    public static void main(String[] args) {

        Observable<Instance<SocketAddress>> hosts = newHostStreamWithCannedLatencies(5L, 1L, 2L, 1L, 0L);

        HttpLoadBalancer<ByteBuf, ByteBuf> lb =
                HttpLoadBalancer.<ByteBuf, ByteBuf>choiceOfTwo(hosts, failureListener -> {
                    return new WeightedHttpClientListener() {

                        private volatile int lastSeenLatencyInverse;

                        @Override
                        public int getWeight() {
                            return lastSeenLatencyInverse;
                        }

                        @Override
                        public void onResponseHeadersReceived(int responseCode, long duration, TimeUnit timeUnit) {
                            /* This is just a demo for how to wire the weight of an instance to the load balancer, it
                             * certainly is not the algorithm to be used in real production applications.
                             */
                            lastSeenLatencyInverse = Integer.MAX_VALUE - (int)duration; // High latency => low weight
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
