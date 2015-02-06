package netflix.ocelli.executor;

import java.util.Collection;
import java.util.List;

import netflix.ocelli.FailureDetectingInstanceFactory;
import netflix.ocelli.HostToClientMapper;
import netflix.ocelli.InstanceCollector;
import netflix.ocelli.LoadBalancer;
import netflix.ocelli.Member;
import netflix.ocelli.MemberToInstance;
import netflix.ocelli.MembershipEvent;
import netflix.ocelli.MembershipEventToMember;
import netflix.ocelli.loadbalancer.RoundRobinLoadBalancer;
import rx.Observable;
import rx.functions.Action1;
import rx.functions.Actions;
import rx.functions.Func1;
import rx.functions.Func2;

public class ExecutorBuilder<H, C, I, O> {
    public static interface Configurator<H, C, I, O> {
        void configure(ExecutorBuilder<H, C, I, O> builder);
    }
    
    private FailureDetectingInstanceFactory.Builder<C> fdBuilder = FailureDetectingInstanceFactory.builder();
    
    private Func1<H, C>                     hostToClient;
    private Observable<Member<H>>           hosts;
    private Func2<C, I, Observable<O>>      operation;
    private Action1<C>                      clientShutdown = Actions.empty();
    private Func1<Observable<List<C>>, LoadBalancer<C>> lbFactory = RoundRobinLoadBalancer.factory();
    private Func2<LoadBalancer<C>, Func2<C, I, Observable<O>>, Executor<I, O>> strategy = SimpleExecutor.factory();

    public ExecutorBuilder<H, C, I, O> withSourceEvent(Observable<MembershipEvent<H>> hosts) {
        this.hosts = hosts.compose(new MembershipEventToMember<H>());
        return this;
    }
    
    public ExecutorBuilder<H, C, I, O> withMemberSource(Observable<Member<H>> hosts) {
        this.hosts = hosts;
        return this;
    }
    
    public ExecutorBuilder<H, C, I, O> withClientFactory(Func1<H, C> hostToClient) {
        this.hostToClient = hostToClient;
        return this;
    }
    
    public ExecutorBuilder<H, C, I, O> withClientConnector(Func1<C, Observable<C>> clientConnector) {
        this.fdBuilder.withClientConnector(clientConnector);
        return this;
    }
    
    public ExecutorBuilder<H, C, I, O> withClientShutdown(Action1<C> clientShutdown) {
        this.clientShutdown = clientShutdown;
        return this;
    }
    
    public ExecutorBuilder<H, C, I, O> withFailureDetector(Func1<C, Observable<Throwable>> failureDetector) {
        fdBuilder.withFailureDetector(failureDetector);
        return this;
    }
    
    public ExecutorBuilder<H, C, I, O> withQuarantineStrategy(Func1<Integer, Long> quarantineStrategy) {
        fdBuilder.withQuarantineStrategy(quarantineStrategy);
        return this;
    }
    
    public ExecutorBuilder<H, C, I, O> withRequestOperation(Func2<C, I, Observable<O>> operation) {
        this.operation = operation;
        return this;
    }
    
    public ExecutorBuilder<H, C, I, O> withLoadBalancer(Func1<Observable<List<C>>, LoadBalancer<C>> factory) {
        this.lbFactory = factory;
        return this;
    }
    
    public ExecutorBuilder<H, C, I, O> withExecutionStrategy(Func2<LoadBalancer<C>, Func2<C, I, Observable<O>>, Executor<I, O>> strategy) {
        this.strategy = strategy;
        return this;
    }
    
    public Executor<I, O> build() {
        MemberToInstance<H, C> memberToInstance = MemberToInstance.from(new HostToClientMapper<H, C>(
                hostToClient, 
                clientShutdown, 
                fdBuilder.build()));
           
        return strategy.call(
                lbFactory.call(
                        hosts
                           .map(memberToInstance)
                           .compose(new InstanceCollector<C>())), 
                operation);

    }
    
    public static <H, C, I, O> ExecutorBuilder<H, C, I, O> builder() {
        return new ExecutorBuilder<H, C, I, O>();
    }
    
    public static <H, C, I, O> Executor<I, O> create(Collection<Configurator<H, C, I, O>> configs) {
        ExecutorBuilder<H, C, I, O> builder = builder();
        for (Configurator<H, C, I, O> config : configs) {
            config.configure(builder);
        }
        return builder.build();
    }
}
