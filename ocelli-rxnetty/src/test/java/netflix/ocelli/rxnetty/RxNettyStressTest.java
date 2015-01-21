package netflix.ocelli.rxnetty;

import io.netty.buffer.ByteBuf;
import io.reactivex.netty.protocol.http.client.FlatResponseOperator;
import io.reactivex.netty.protocol.http.client.HttpClientRequest;
import io.reactivex.netty.protocol.http.client.HttpClientResponse;
import io.reactivex.netty.protocol.http.client.ResponseHolder;

import java.nio.charset.Charset;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import netflix.ocelli.LoadBalancer;
import netflix.ocelli.MembershipEvent;
import netflix.ocelli.MembershipEvent.EventType;
import netflix.ocelli.MembershipFailureDetector;
import netflix.ocelli.execute.BackupRequestExecutionStrategy;
import netflix.ocelli.functions.Limiters;
import netflix.ocelli.loadbalancer.ChoiceOfTwoLoadBalancer;
import netflix.ocelli.stats.ExponentialAverage;

import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.uncommons.maths.random.ExponentialGenerator;

import rx.Observable;
import rx.Observer;
import rx.functions.Action1;
import rx.functions.Func0;
import rx.functions.Func1;
import rx.functions.Func2;

@Ignore
public class RxNettyStressTest {
    private static final Logger LOG = LoggerFactory.getLogger(RxNettyStressTest.class);
    
    public static int OPS_PER_SECOND = 1000;
    public static int SERVER_COUNT   = 5;
    public static long interval      = 1000000 / OPS_PER_SECOND;
    
    @ClassRule
    public static NettyServerFarmResource servers = new NettyServerFarmResource(SERVER_COUNT);
    
    @Rule
    public TestName name = new TestName();
    
    @Test
    @Ignore
    public void stressTest() throws InterruptedException {
        final PoolHttpMetricListener poolListener = new PoolHttpMetricListener();
        final HttpClientPool<ByteBuf, ByteBuf> clientPool = HttpClientPool.newPool();
        
        final LoadBalancer<HttpClientHolder<ByteBuf, ByteBuf>> lb =
                ChoiceOfTwoLoadBalancer
                    .from(servers
                        .hosts()
                        .map(clientPool.toFunc())
                        .map(HttpClientHolder.<ByteBuf, ByteBuf>toHolder(ExponentialAverage.factory(100, 10), poolListener))
                        .map(MembershipEvent.<HttpClientHolder<ByteBuf, ByteBuf>>toEvent(EventType.ADD))
                        .lift(MembershipFailureDetector.<HttpClientHolder<ByteBuf, ByteBuf>>builder()
                            .withFailureDetector(new RxNettyFailureDetector<ByteBuf, ByteBuf>())
                            .build()),
                    new Func2<HttpClientHolder<ByteBuf, ByteBuf>, HttpClientHolder<ByteBuf, ByteBuf>, HttpClientHolder<ByteBuf, ByteBuf>>() {
                        @Override
                        public HttpClientHolder<ByteBuf, ByteBuf> call(
                                HttpClientHolder<ByteBuf, ByteBuf> left,
                                HttpClientHolder<ByteBuf, ByteBuf> right) {
                            return left.getListener().getAverageLatency() > right.getListener().getAverageLatency()
                                ? left 
                                : right;
                        }
                    });

        final BackupRequestExecutionStrategy<HttpClientHolder<ByteBuf, ByteBuf>> execution = BackupRequestExecutionStrategy
                .builder(lb)
                .withBackupTimeout(new Func0<Integer>() {
                    @Override
                    public Integer call() {
                        return poolListener.getLatencyPercentile(0.90);
                    }
                }, TimeUnit.MILLISECONDS)
                .withLimiter(Limiters.exponential(0.90, 20))
                .build();

        final AtomicLong counter = new AtomicLong();
        final ExponentialGenerator generator = new ExponentialGenerator(10.0, new Random());

        Observable.interval(interval, TimeUnit.MICROSECONDS)
            .flatMap(new Func1<Long, Observable<String>>() {
                @Override
                public Observable<String> call(Long t1) {
                    final long startTime = System.currentTimeMillis();
                    final CopyOnWriteArrayList<Integer> delays = new CopyOnWriteArrayList<Integer>();
                    return execution.execute(new Func1<HttpClientHolder<ByteBuf, ByteBuf>, Observable<HttpClientResponse<ByteBuf>>>() {
                            @Override
                            public Observable<HttpClientResponse<ByteBuf>> call(final HttpClientHolder<ByteBuf, ByteBuf> holder) {
                                int delay = (int) (generator.nextValue() * 1000);
                                delays.add(delay);
                                HttpClientRequest<ByteBuf> request = HttpClientRequest.createGet("?delay="+delay);
                                return holder.getClient().submit(request);
                            }
                        })
                        .lift(FlatResponseOperator.<ByteBuf>flatResponse())
                        .map(new Func1<ResponseHolder<ByteBuf>, String>() {
                            @Override
                            public String call(ResponseHolder<ByteBuf> holder) {
                                counter.incrementAndGet();
                                return holder.getContent().toString(Charset.defaultCharset());
                            }
                        })
                        .doOnNext(new Action1<String>() {
                            @Override
                            public void call(String t1) {
                                final long endTime = System.currentTimeMillis();
                                // LOG.info("Actual " + (endTime - startTime) + " from " + delays);
                            }
                        });
                }
            })
            .subscribe(new Observer<String>() {
                @Override
                public void onCompleted() {
                }

                @Override
                public void onError(Throwable e) {
                    e.printStackTrace();
                }

                @Override
                public void onNext(String t) {
                    // LOG.info(" Result : " + t1);
                }
            });

        Observable.interval(1, TimeUnit.SECONDS)
            .subscribe(new Action1<Long>() {
                @Override
                public void call(Long t1) {
                    long current = counter.getAndSet(0);
                    LOG.info("Rate: {} / sec.   95th: {}   50th: {}  Ratio : {}", 
                            current, 
                            poolListener.getLatencyPercentile(0.90), 
                            poolListener.getLatencyPercentile(0.50));
                }
            });

        TimeUnit.SECONDS.sleep(100);
    }
}
