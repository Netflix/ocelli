package netflix.ocelli.rxnetty;

import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
//        final Func0<SingleMetric<Long>> averageFactory = ExponentialAverage.factory(100, 10);
//        
//        PoolingHostToClientLifecycleFactory<Host, HttpClientHolder<ByteBuf, ByteBuf>> factory = 
//            new PoolingHostToClientLifecycleFactory<Host, HttpClientHolder<ByteBuf, ByteBuf>>(
//                new HostToClient<Host, HttpClientHolder<ByteBuf, ByteBuf>>() {
//                    @Override
//                    public HttpClientHolder<ByteBuf, ByteBuf> call(Host host) {
//                        return new HttpClientHolder<ByteBuf, ByteBuf>(
//                                RxNetty.createHttpClient(host.getHostName(), host.getPort()), 
//                                averageFactory.call());
//                    }
//                }, 
//                FailureDetectingClientLifecycleFactory.<HttpClientHolder<ByteBuf, ByteBuf>>builder()
//                    .withQuarantineStrategy(Delays.fixed(1, TimeUnit.SECONDS))
//                    .withFailureDetector(new RxNettyFailureDetector<ByteBuf, ByteBuf>())
//                    .withClientShutdown(new Action1<HttpClientHolder<ByteBuf, ByteBuf>>() {
//                        @Override
//                        public void call(HttpClientHolder<ByteBuf, ByteBuf> t1) {
//                            t1.getClient().shutdown();
//                        }
//                    })
//                    .build());           
//        
//        final LoadBalancer<HttpClientHolder<ByteBuf, ByteBuf>> lb =
//                ChoiceOfTwoLoadBalancer.create(
//                    servers
//                        .hosts()
//                        .map(MembershipEvent.<Host>toEvent(EventType.ADD))
//                        .lift(HostToClientCollector.create(factory)),
//                    new Func2<HttpClientHolder<ByteBuf, ByteBuf>, HttpClientHolder<ByteBuf, ByteBuf>, HttpClientHolder<ByteBuf, ByteBuf>>() {
//                        @Override
//                        public HttpClientHolder<ByteBuf, ByteBuf> call(
//                                HttpClientHolder<ByteBuf, ByteBuf> left,
//                                HttpClientHolder<ByteBuf, ByteBuf> right) {
//                            return left.getListener().getMetric() > right.getListener().getMetric()
//                                ? left 
//                                : right;
//                        }
//                    });
//
//        final BackupRequestExecutionStrategy<HttpClientHolder<ByteBuf, ByteBuf>> execution = BackupRequestExecutionStrategy
//                .builder(lb)
//                .withTimeoutMetric(Metrics.quantile(0.90))
//                .withLimiter(Limiters.exponential(0.90, 20))
//                .build();
//
//        final AtomicLong counter = new AtomicLong();
//        final ExponentialGenerator generator = new ExponentialGenerator(10.0, new Random());
//
//        Observable.interval(interval, TimeUnit.MICROSECONDS)
//            .flatMap(new Func1<Long, Observable<String>>() {
//                @Override
//                public Observable<String> call(Long t1) {
//                    final long startTime = System.currentTimeMillis();
//                    final CopyOnWriteArrayList<Integer> delays = new CopyOnWriteArrayList<Integer>();
//                    return execution.execute(new Func1<HttpClientHolder<ByteBuf, ByteBuf>, Observable<HttpClientResponse<ByteBuf>>>() {
//                            @Override
//                            public Observable<HttpClientResponse<ByteBuf>> call(final HttpClientHolder<ByteBuf, ByteBuf> holder) {
//                                int delay = (int) (generator.nextValue() * 1000);
//                                delays.add(delay);
//                                HttpClientRequest<ByteBuf> request = HttpClientRequest.createGet("?delay="+delay);
//                                return holder.getClient().submit(request);
//                            }
//                        })
//                        .lift(FlatResponseOperator.<ByteBuf>flatResponse())
//                        .map(new Func1<ResponseHolder<ByteBuf>, String>() {
//                            @Override
//                            public String call(ResponseHolder<ByteBuf> holder) {
//                                counter.incrementAndGet();
//                                return holder.getContent().toString(Charset.defaultCharset());
//                            }
//                        })
//                        .doOnNext(new Action1<String>() {
//                            @Override
//                            public void call(String t1) {
//                                final long endTime = System.currentTimeMillis();
//                                // LOG.info("Actual " + (endTime - startTime) + " from " + delays);
//                            }
//                        });
//                }
//            })
//            .subscribe(new Observer<String>() {
//                @Override
//                public void onCompleted() {
//                }
//
//                @Override
//                public void onError(Throwable e) {
//                    e.printStackTrace();
//                }
//
//                @Override
//                public void onNext(String t) {
//                    // LOG.info(" Result : " + t1);
//                }
//            });
//
//        Observable.interval(1, TimeUnit.SECONDS)
//            .subscribe(new Action1<Long>() {
//                @Override
//                public void call(Long t1) {
//                    long current = counter.getAndSet(0);
//                    LOG.info("Rate: {} / sec.", current);
//                }
//            });
//
//        TimeUnit.SECONDS.sleep(100);
    }
}
