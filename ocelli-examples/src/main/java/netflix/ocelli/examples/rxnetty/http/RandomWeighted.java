package netflix.ocelli.examples.rxnetty.http;

import io.netty.buffer.ByteBuf;
import io.reactivex.netty.protocol.http.client.HttpClient;
import netflix.ocelli.Instance;
import netflix.ocelli.examples.rxnetty.http.HttpExampleUtils.*;
import netflix.ocelli.rxnetty.protocol.http.HttpLoadBalancer;
import netflix.ocelli.rxnetty.protocol.http.WeightedHttpClientListener;
import rx.Observable;

import java.net.ConnectException;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.charset.Charset;
import java.util.concurrent.TimeUnit;

import static netflix.ocelli.examples.rxnetty.http.HttpExampleUtils.*;

public final class RandomWeighted {

    private RandomWeighted() {
    }

    public static void main(String[] args) {

        Observable<Instance<SocketAddress>> hosts = newHostStreamWithCannedLatencies(1L, 2L);

        HttpLoadBalancer<ByteBuf, ByteBuf> lb =
                HttpLoadBalancer.<ByteBuf, ByteBuf>weigthedRandom(hosts, failureListener -> {
                    return new WeightedHttpClientListener() {

                        private volatile int weight;

                        @Override
                        public int getWeight() {
                            return weight;
                        }

                        @Override
                        public void onResponseHeadersReceived(int responseCode, long duration, TimeUnit timeUnit) {
                            /* This is just a demo for how to wire the weight of an instance to the load balancer, it
                             * certainly is not the algorithm to be used in real production applications.
                             */
                            weight = (int) (Long.MAX_VALUE - duration); // High latency => low weight

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
