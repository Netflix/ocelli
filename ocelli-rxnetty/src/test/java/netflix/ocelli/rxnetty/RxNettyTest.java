package netflix.ocelli.rxnetty;

import io.netty.buffer.ByteBuf;
import io.reactivex.netty.protocol.http.client.FlatResponseOperator;
import io.reactivex.netty.protocol.http.client.ResponseHolder;

import java.nio.charset.Charset;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import netflix.ocelli.HostAddress;
import netflix.ocelli.HostEvent;
import netflix.ocelli.HostEvent.EventType;
import netflix.ocelli.ManagedLoadBalancer;
import netflix.ocelli.loadbalancer.DefaultLoadBalancer;
import netflix.ocelli.retrys.Retrys;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;
import rx.functions.Action1;
import rx.functions.Func1;

import com.google.common.collect.Lists;

public class RxNettyTest {
    private static final Logger LOG = LoggerFactory.getLogger(RxNettyTest.class);
    
//    @ClassRule
//    public static NettyServerFarmResource servers = new NettyServerFarmResource(100);
    
    public static int OPS_PER_SECOND = 2000;
    public static int SERVER_COUNT = 50;
    public static long interval = 1000000 / OPS_PER_SECOND;
    
    @Test
    public void stressTest() throws InterruptedException {
        List<HostAddress> si = Lists.newArrayList();
        for (int i = 0; i < SERVER_COUNT; i++) {
            si.add(new HostAddress().setHost("localhost").setPort(8080+i));
        }
        
        final ManagedLoadBalancer<HostAddress, RxNettyHttpClient> lb = 
                DefaultLoadBalancer.<HostAddress, RxNettyHttpClient>builder()
                .withHostSource(Observable.from(si).map(HostEvent.<HostAddress>toEvent(EventType.ADD)))
                .withClientConnector(RxNettyClientConnector.builder().build())
                .build();

        lb.initialize();
        
        final AtomicLong counter = new AtomicLong();
        
        Observable.interval(interval, TimeUnit.MICROSECONDS)
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
