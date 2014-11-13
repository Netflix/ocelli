package netflix.ocelli.rxnetty;

import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.reactivex.netty.RxNetty;
import io.reactivex.netty.protocol.http.client.FlatResponseOperator;
import io.reactivex.netty.protocol.http.client.HttpClient;
import io.reactivex.netty.protocol.http.client.HttpClientRequest;
import io.reactivex.netty.protocol.http.client.ResponseHolder;
import netflix.ocelli.Host;
import netflix.ocelli.LoadBalancer;
import netflix.ocelli.LoadBalancers;
import netflix.ocelli.MembershipEvent;
import netflix.ocelli.MembershipEvent.EventType;
import netflix.ocelli.algorithm.LinearWeightingStrategy;
import netflix.ocelli.functions.Retrys;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.functions.Action1;
import rx.functions.Func1;

import java.nio.charset.Charset;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class RxNettyTest {
    private static final Logger LOG = LoggerFactory.getLogger(RxNettyTest.class);
    
//    @ClassRule
//    public static NettyServerFarmResource servers = new NettyServerFarmResource(100);
    
    public static int OPS_PER_SECOND = 1000;
    public static int SERVER_COUNT = 5;
    public static long interval = 1000000 / OPS_PER_SECOND;
    
    @Test
    @Ignore
    public void stressTest() throws InterruptedException {
        List<Host> si = Lists.newArrayList();

        for (int i = 0; i < SERVER_COUNT; i++) {
            si.add(new Host("localhost", 8080+i));
        }

        Observable<HttpClient<ByteBuf, ByteBuf>> clientSource = Observable.from(si)
                                                                 .map(new Func1<Host, HttpClient<ByteBuf, ByteBuf>>() {
                                                                     @Override
                                                                     public HttpClient<ByteBuf, ByteBuf> call(Host host) {
                                                                         return RxNetty.createHttpClient(host.getHostName(), host.getPort());
                                                                     }
                                                                 });

        final LoadBalancer<HttpClientHolder<ByteBuf, ByteBuf>> lb =
                LoadBalancers.newBuilder(clientSource
                                                 .map(new Func1<HttpClient<ByteBuf, ByteBuf>, MembershipEvent<HttpClientHolder<ByteBuf, ByteBuf>>>() {
                                                     @Override
                                                     public MembershipEvent<HttpClientHolder<ByteBuf, ByteBuf>> call(
                                                             HttpClient<ByteBuf, ByteBuf> client) {
                                                         return new MembershipEvent<HttpClientHolder<ByteBuf, ByteBuf>>(
                                                                 EventType.ADD, new HttpClientHolder<ByteBuf, ByteBuf>(
                                                                 client));
                                                     }
                                                 }))
                .withWeightingStrategy(new LinearWeightingStrategy<HttpClientHolder<ByteBuf, ByteBuf>>(
                        new RxNettyPendingRequests<ByteBuf, ByteBuf>()))
                        .build();

        final AtomicLong counter = new AtomicLong();
        
        Observable.interval(interval, TimeUnit.MICROSECONDS)
            .subscribe(new Action1<Long>() {
                @Override
                public void call(Long t1) {
                    lb
                    .choose()
                    .concatMap(new Func1<HttpClientHolder<ByteBuf, ByteBuf>, Observable<String>>() {
                        @Override
                        public Observable<String> call(HttpClientHolder<ByteBuf, ByteBuf> holder) {
                            HttpClientRequest<ByteBuf> request = HttpClientRequest.createGet("/hello");
                            return holder.getClient().submit(request)
                                .lift(FlatResponseOperator.<ByteBuf>flatResponse())
                                .map(new Func1<ResponseHolder<ByteBuf>, String>() {
                                    @Override
                                    public String call(ResponseHolder<ByteBuf> holder) {
                                        counter.incrementAndGet();
                                        return holder.getContent().toString(Charset.defaultCharset());
                                    }
                                });
                        }
                    })
                    .retryWhen(Retrys.exponentialBackoff(3, 1, TimeUnit.SECONDS))
                    .subscribe();                            
                }
            });

        Observable.interval(1, TimeUnit.SECONDS)
            .subscribe(new Action1<Long>() {
                @Override
                public void call(Long t1) {
                    long current = counter.getAndSet(0);
                    LOG.info("Rate " + current + " / sec");
                }
            });

        TimeUnit.SECONDS.sleep(100);
    }
}
