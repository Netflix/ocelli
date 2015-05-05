package netflix.ocelli;

import java.util.Comparator;
import java.util.concurrent.TimeUnit;

import netflix.ocelli.InstanceQuarantiner.IncarnationFactory;
import netflix.ocelli.functions.Delays;
import netflix.ocelli.loadbalancer.ChoiceOfTwoLoadBalancer;
import netflix.ocelli.loadbalancer.RoundRobinLoadBalancer;

import org.junit.Assert;
import org.junit.Test;

import rx.Observable;
import rx.functions.Func1;
import rx.schedulers.TestScheduler;

import com.google.common.collect.Lists;

public class LoadBalancerTest {
    @Test
    public void createFromFixedList() {
        LoadBalancer<String> lb = LoadBalancer
            .fromFixedSource(Lists.newArrayList("host1:8080", "host2:8080"))
            .build(RoundRobinLoadBalancer.<String>create(0))
            ;
        
        Assert.assertEquals("host1:8080", lb.next());
        Assert.assertEquals("host2:8080", lb.next());
    }
    
    class ClientWithWeight {
        private String address;
        private Integer weight;
        
        public ClientWithWeight(String address, int weight) {
            this.address = address;
            this.weight = weight;
        }
    }
    
    Comparator<ClientWithWeight> COMPARE_BY_WEIGHT = new Comparator<ClientWithWeight>() {
        @Override
        public int compare(ClientWithWeight o1, ClientWithWeight o2) {
            return o1.weight.compareTo(o2.weight);
        }
    };

    Func1<Instance<String>, Instance<ClientWithWeight>> CLIENT_FROM_ADDRESS = new Func1<Instance<String>, Instance<ClientWithWeight>>() {
        @Override
        public Instance<ClientWithWeight> call(Instance<String> t1) {
            return Instance.create(new ClientWithWeight(t1.getValue(), 1), t1.getLifecycle());
        }
    };

    @Test
    public void createFromFixedListAndConvertToDifferentType() {
        LoadBalancer<ClientWithWeight> lb = LoadBalancer
            .fromFixedSource(Lists.newArrayList("host1:8080", "host2:8080"))
            .convertTo(CLIENT_FROM_ADDRESS)
            .build(RoundRobinLoadBalancer.<ClientWithWeight>create(0))
            ;
        
        Assert.assertEquals("host2:8080", lb.next().address);
        Assert.assertEquals("host1:8080", lb.next().address);
    }
    
    @Test
    public void createFromFixedListWithAdvancedAlgorithm() {
        LoadBalancer<ClientWithWeight> lb = LoadBalancer
            .fromFixedSource(Lists.newArrayList(new ClientWithWeight("host1:8080", 1), new ClientWithWeight("host2:8080", 2)))
            .build(ChoiceOfTwoLoadBalancer.<ClientWithWeight>create(COMPARE_BY_WEIGHT))
            ;
            
        Assert.assertEquals("host2:8080", lb.next().address);
        Assert.assertEquals("host2:8080", lb.next().address);
    }
    
    class ClientWithLifecycle {
        private String address;
        private InstanceEventListener listener;
        
        public ClientWithLifecycle(String address) {
            this.address = address;
            this.listener = null;
        }
        
        public ClientWithLifecycle(ClientWithLifecycle parent, InstanceEventListener listener) {
            this.address  = parent.address;
            this.listener = listener;
        }
        
        public void forceFail() {
            listener.onEvent(InstanceEvent.EXECUTION_FAILED, 0, TimeUnit.MILLISECONDS, new Throwable("Failed"), null);
        }
    }
    

    @Test
    public void createFromFixedAndUseQuaratiner() {
        TestScheduler scheduler = new TestScheduler();
        
        LoadBalancer<ClientWithLifecycle> lb = LoadBalancer
                .fromFixedSource(Lists.newArrayList(new ClientWithLifecycle("host1:8080"), new ClientWithLifecycle("host2:8080")))
                .withQuarantiner(new IncarnationFactory<ClientWithLifecycle>() {
                    @Override
                    public ClientWithLifecycle create(ClientWithLifecycle client,
                            InstanceEventListener listener,
                            Observable<Void> lifecycle) {
                        return new ClientWithLifecycle(client, listener);
                    }
                }, Delays.fixed(10, TimeUnit.SECONDS), scheduler)
                .build(RoundRobinLoadBalancer.<ClientWithLifecycle>create(0));
        
        ClientWithLifecycle clientToFail = lb.next();
        Assert.assertEquals("host2:8080", clientToFail.address);
        clientToFail.forceFail();
        
        Assert.assertEquals("host1:8080", lb.next().address);
        Assert.assertEquals("host1:8080", lb.next().address);

    }
}
