package netflix.ocelli;

import com.google.common.collect.Lists;
import netflix.ocelli.InstanceQuarantiner.IncarnationFactory;
import netflix.ocelli.functions.Delays;
import netflix.ocelli.loadbalancer.ChoiceOfTwoLoadBalancer;
import netflix.ocelli.loadbalancer.RoundRobinLoadBalancer;
import org.hamcrest.MatcherAssert;
import org.junit.Test;
import rx.Observable;
import rx.Subscription;
import rx.functions.Func0;
import rx.functions.Func1;
import rx.schedulers.TestScheduler;

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.*;

public class LoadBalancerTest {

    private static final Comparator<ClientWithWeight> COMPARE_BY_WEIGHT = new Comparator<ClientWithWeight>() {
        @Override
        public int compare(ClientWithWeight o1, ClientWithWeight o2) {
            return o1.weight.compareTo(o2.weight);
        }
    };

    private static final Func1<Instance<String>, Instance<ClientWithWeight>> CLIENT_FROM_ADDRESS = new Func1<Instance<String>, Instance<ClientWithWeight>>() {
        @Override
        public Instance<ClientWithWeight> call(Instance<String> t1) {
            return Instance.create(new ClientWithWeight(t1.getValue(), 1), t1.getLifecycle());
        }
    };

    @Test
    public void createFromFixedList() {
        LoadBalancer<String> lb = LoadBalancer
                .fromFixedSource(Lists.newArrayList("host1:8080", "host2:8080"))
                .build(RoundRobinLoadBalancer.<String>create(0),
                       InstanceCollector.create(new Func0<Map<String, Subscription>>() {
                           @Override
                           public Map<String, Subscription> call() {
                               return new LinkedHashMap<String, Subscription>();
                           }
                       }))
                ;

        MatcherAssert.assertThat("Unexpected first host chosen.", lb.next(), is("host2:8080"));
        MatcherAssert.assertThat("Unexpected second host chosen.", lb.next(), is("host1:8080"));
    }

    @Test
    public void createFromFixedListAndConvertToDifferentType() {
        LoadBalancer<ClientWithWeight> lb = LoadBalancer
            .fromFixedSource(Lists.newArrayList("host1:8080", "host2:8080"))
            .convertTo(CLIENT_FROM_ADDRESS)
            .build(RoundRobinLoadBalancer.<ClientWithWeight>create(0),
                   InstanceCollector.create(new Func0<Map<ClientWithWeight, Subscription>>() {
                        @Override
                        public Map<ClientWithWeight, Subscription> call() {
                            return new LinkedHashMap<ClientWithWeight, Subscription>();
                        }
                    }))
            ;

        MatcherAssert.assertThat("Unexpected first host chosen.", lb.next().address, equalTo("host2:8080"));
        MatcherAssert.assertThat("Unexpected second host chosen.", lb.next().address, equalTo("host1:8080"));
    }
    
    @Test
    public void createFromFixedListWithAdvancedAlgorithm() {
        LoadBalancer<ClientWithWeight> lb = LoadBalancer
            .fromFixedSource(
                    Lists.newArrayList(new ClientWithWeight("host1:8080", 1), new ClientWithWeight("host2:8080", 2)))
            .build(ChoiceOfTwoLoadBalancer.create(COMPARE_BY_WEIGHT),
                   InstanceCollector.create(new Func0<Map<ClientWithWeight, Subscription>>() {
                       @Override
                       public Map<ClientWithWeight, Subscription> call() {
                           return new LinkedHashMap<ClientWithWeight, Subscription>();
                       }
                   }))
            ;

        MatcherAssert.assertThat("Unexpected first host chosen.", lb.next().address, equalTo("host2:8080"));
        MatcherAssert.assertThat("Unexpected second host chosen.", lb.next().address, equalTo("host2:8080"));
    }

    @Test
    public void createFromFixedAndUseQuaratiner() {
        TestScheduler scheduler = new TestScheduler();

        LoadBalancer<ClientWithLifecycle> lb = LoadBalancer
                .fromFixedSource(Lists.newArrayList(new ClientWithLifecycle("host1:8080"),
                                                    new ClientWithLifecycle("host2:8080")))
                .withQuarantiner(new IncarnationFactory<ClientWithLifecycle>() {
                    @Override
                    public ClientWithLifecycle create(ClientWithLifecycle client,
                                                      InstanceEventListener listener,
                                                      Observable<Void> lifecycle) {
                        return new ClientWithLifecycle(client, listener);
                    }
                }, Delays.fixed(10, TimeUnit.SECONDS), scheduler)
                .build(RoundRobinLoadBalancer.<ClientWithLifecycle>create(0),
                       InstanceCollector.create(new Func0<Map<ClientWithLifecycle, Subscription>>() {
                           @Override
                           public Map<ClientWithLifecycle, Subscription> call() {
                               return new LinkedHashMap<ClientWithLifecycle, Subscription>();
                           }
                       }));

        ClientWithLifecycle clientToFail = lb.next();
        MatcherAssert.assertThat("Unexpected host chosen before failure.", clientToFail.address, equalTo("host2:8080"));
        clientToFail.forceFail();

        MatcherAssert.assertThat("Unexpected first host chosen post failure.", lb.next().address, equalTo("host1:8080"));
        MatcherAssert.assertThat("Unexpected second host chosen post failure.", lb.next().address, equalTo("host1:8080"));
    }

    static class ClientWithLifecycle {
        private final String address;
        private final InstanceEventListener listener;
        
        public ClientWithLifecycle(String address) {
            this.address = address;
            listener = null;
        }
        
        public ClientWithLifecycle(ClientWithLifecycle parent, InstanceEventListener listener) {
            address = parent.address;
            this.listener = listener;
        }
        
        public void forceFail() {
            listener.onEvent(InstanceEvent.EXECUTION_FAILED, 0, TimeUnit.MILLISECONDS, new Throwable("Failed"), null);
        }
    }


    static class ClientWithWeight {
        private final String address;
        private final Integer weight;

        public ClientWithWeight(String address, int weight) {
            this.address = address;
            this.weight = weight;
        }
    }
}
