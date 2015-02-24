package netflix.ocelli.executor;

import java.util.Collection;

import netflix.ocelli.CachingInstanceTransformer;
import netflix.ocelli.FailureDetectingInstanceFactory;
import netflix.ocelli.Instance;
import netflix.ocelli.InstanceCollector;
import netflix.ocelli.LoadBalancer;
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
    private Observable<Instance<H>>         instances;
    private Func2<C, I, Observable<O>>      operation;
    private Action1<C>                      clientShutdown = Actions.empty();
    private LoadBalancer<C>                 lb = RoundRobinLoadBalancer.create();
    private Func2<LoadBalancer<C>, Func2<C, I, Observable<O>>, Executor<I, O>> strategy = SimpleExecutor.factory();

    public ExecutorBuilder<H, C, I, O> withInstances(Observable<Instance<H>> hosts) {
        this.instances = hosts;
        return this;
    }
    
    public ExecutorBuilder<H, C, I, O> withClientFactory(Func1<H, C> hostToClient) {
        this.hostToClient = hostToClient;
        return this;
    }
    
    public ExecutorBuilder<H, C, I, O> withClientConnector(Func1<C, Observable<Void>> clientConnector) {
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
    
    public ExecutorBuilder<H, C, I, O> withLoadBalancer(LoadBalancer<C> lb) {
        this.lb = lb;
        return this;
    }
    
    public ExecutorBuilder<H, C, I, O> withExecutionStrategy(Func2<LoadBalancer<C>, Func2<C, I, Observable<O>>, Executor<I, O>> strategy) {
        this.strategy = strategy;
        return this;
    }
    
    public Executor<I, O> build() {
        CachingInstanceTransformer<H, C> memberToInstance = CachingInstanceTransformer.from(
                hostToClient,
                clientShutdown,
                fdBuilder.build());

        instances
            .map(memberToInstance)
            .compose(new InstanceCollector<C>())
            .subscribe(this.lb);

        return strategy.call(lb, operation);
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
