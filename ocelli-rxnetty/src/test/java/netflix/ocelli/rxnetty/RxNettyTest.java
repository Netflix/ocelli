package netflix.ocelli.rxnetty;

import io.netty.buffer.ByteBuf;
import io.reactivex.netty.protocol.http.client.FlatResponseOperator;
import io.reactivex.netty.protocol.http.client.ResponseHolder;

import java.nio.charset.Charset;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import netflix.ocelli.LoadBalancer;
import netflix.ocelli.loadbalancer.DefaultLoadBalancer;
import netflix.ocelli.metrics.ClientMetrics;
import netflix.ocelli.metrics.SimpleClientMetricsFactory;
import netflix.ocelli.retrys.Retrys;

import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;
import rx.functions.Action1;
import rx.functions.Func1;

public class RxNettyTest {
    private static final Logger LOG = LoggerFactory.getLogger(RxNettyTest.class);
    
    @ClassRule
    public static NettyServerFarmResource servers = new NettyServerFarmResource(100);
    
    @Test
    public void stressTest() throws InterruptedException {
        final LoadBalancer<ServerInfo, RxNettyHttpClient, ClientMetrics> lb = 
                DefaultLoadBalancer.<ServerInfo, RxNettyHttpClient, ClientMetrics>builder()
                .withHostSource(servers.hostEvents())
                .withClientConnector(RxNettyClientConnector.builder().build())
                .withMetricsFactory(new SimpleClientMetricsFactory<ServerInfo>())
                .build();

        lb.initialize();
        
        final AtomicLong counter = new AtomicLong();
        
        // 100 Threads
        Observable.range(0, 100)
            .subscribe(new Action1<Integer>() {
                @Override
                public void call(Integer t1) {
                    // Operation every 100 millis
                    Observable.interval(100, TimeUnit.MILLISECONDS)
                        .subscribe(new Action1<Long>() {
                            @Override
                            public void call(Long t1) {
                                lb
                                .choose()
                                .concatMap(new Func1<RxNettyHttpClient, Observable<String>>() {
                                    @Override
                                    public Observable<String> call(RxNettyHttpClient client) {
                                        return client
                                            .createGet("/hello")
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
