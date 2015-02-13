package netflix.ocelli;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Notification;
import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Subscriber;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.functions.Func2;
import rx.subjects.BehaviorSubject;
import rx.subscriptions.Subscriptions;

/**
 * Mapping function with cache to convert an Instance<T> of one type to an Instance<S> of 
 * another type.  InstanceTransformer is meant to be used across multiple Instance streams 
 * when these streams may having overlapping instances so that only one Instance<S> is 
 * created for each unique T. The returned Instance<S> is a proxy to the single Instance<S> 
 * so that it may be cleaned up when all the source Instance<T> instances have been removed.
 * 
 * @author elandau
 *
 * @param <T>
 * @param <S>
 */
public class CachingInstanceTransformer<T, S> implements Func1<Instance<T>, Instance<S>> {
    private static final Logger LOG = LoggerFactory.getLogger(CachingInstanceTransformer.class);
    
    public static <T, S> CachingInstanceTransformer<T, S> from(Func1<T, S> mapperFunc, Action1<S> shutdownFunc, Func1<S, Instance<S>> createFunc) {
        return new CachingInstanceTransformer<T, S>(mapperFunc, shutdownFunc, createFunc);
    }
    
    private final Func1<S, Instance<S>> createFunc;
    private final Func1<T, S> mapperFunc;
    private final Action1<S> shutdownFunc;
    
    private final ConcurrentMap<T, RefCountedInstance<S>> cache = new ConcurrentHashMap<T, RefCountedInstance<S>>();
    
    public static <T, S> CachingInstanceTransformer<T, S> create(Func1<T, S> mapperFunc, Action1<S> shutdownFunc, Func1<S, Instance<S>> createFunc) {
        return new CachingInstanceTransformer<T, S>(mapperFunc, shutdownFunc, createFunc);
    }
    
    public static <T> CachingInstanceTransformer<T, T> create(Action1<T> shutdownFunc, Func1<T, Instance<T>> createFunc) {
        return create(
            new Func1<T, T>() {
                @Override
                public T call(T t1) {
                    return t1;
                }
            }, 
            shutdownFunc, 
            createFunc);
    }
    
    public static <T> CachingInstanceTransformer<T, T> create(Func1<T, Instance<T>> createFunc) {
        return new CachingInstanceTransformer<T, T>(
            new Func1<T, T>() {
                @Override
                public T call(T t1) {
                    return t1;
                }
            }, 
            new Action1<T>() {
                @Override
                public void call(T t1) {
                }
            }, 
            createFunc);
    }
    
    public static <T> CachingInstanceTransformer<T, T> create() {
        return new CachingInstanceTransformer<T, T>(
            new Func1<T, T>() {
                @Override
                public T call(T t1) {
                    return t1;
                }
            }, 
            new Action1<T>() {
                @Override
                public void call(T t1) {
                }
            }, 
            new Func1<T, Instance<T>>() {
                @Override
                public Instance<T> call(T t1) {
                    return MutableInstance.from(t1, BehaviorSubject.create(true));
                }
            });
    }
    
    /**
     * Reference counted Instance that calls a shutdown action once all subscriptions
     * have been removed.
     * 
     * @author elandau
     *
     * @param <S>
     */
    private static class RefCountedInstance<S> extends Instance<S> {
        public RefCountedInstance(final Instance<S> delegate, final Action0 remove) {
            super(delegate.getValue(), new OnSubscribe<Boolean>() {
                private AtomicInteger refCount = new AtomicInteger();
                
                @Override
                public void call(Subscriber<? super Boolean> s) {
                    refCount.incrementAndGet();
                    delegate.subscribe(s);
                    s.add(Subscriptions.create(new Action0() {
                        @Override
                        public void call() {
                            if (refCount.decrementAndGet() == 0) {
                                remove.call();
                            }
                        }
                    }));
                }
            });
        }
    }
    
    public CachingInstanceTransformer(Func1<T, S> mapperFunc, Action1<S> shutdownFunc, Func1<S, Instance<S>> createFunc) {
        this.createFunc = createFunc;
        this.mapperFunc = mapperFunc;
        this.shutdownFunc = shutdownFunc;
    }
    
    /**
     * Internal method to create a new instance or get an existing one.  The instance is
     * removed from the cache once all subscription have been unsubscribed.
     * @param member
     * @return
     */
    private Instance<S> getOrCreateInstance(final Instance<T> member) {
        RefCountedInstance<S> instance = cache.get(member.getValue());
        if (instance == null) {
            LOG.info("Created instance " + member);
            final S newMember = mapperFunc.call(member.getValue());
            instance = new RefCountedInstance<S>(createFunc.call(newMember), new Action0() {
                @Override
                public void call() {
                    LOG.info("Destroy instance " + member);
                    cache.remove(member.getValue());
                    shutdownFunc.call(newMember);
                }
            });
            RefCountedInstance<S> existing = cache.putIfAbsent(member.getValue(), instance);
            if (existing != null) {
                instance.subscribe().unsubscribe();
                instance = existing;
            }
        }
        return instance;
    }
    
    @Override
    public Instance<S> call(final Instance<T> member) {
        final Instance<S> instance = getOrCreateInstance(member);
        
        return new Instance<S>(instance.getValue(), new OnSubscribe<Boolean>() {
            @Override
            public void call(final Subscriber<? super Boolean> s) {
                // AND the state of the source and transformed Instance (which may be attached to a 
                // failure detector) to determine the up state.  The 'member' onCompleted event
                // is used as the only indicator that the instance has been removed and the transformed
                // Instance may be unsubscribed.
                Observable.combineLatest(
                    member.materialize(), instance.materialize(), 
                    new Func2<Notification<Boolean>, Notification<Boolean>, Notification<Boolean>>() {
                        @Override
                        public Notification<Boolean> call(Notification<Boolean> primary, Notification<Boolean> secondary) {
                            if (primary.isOnCompleted() || secondary.isOnCompleted()) {
                                return primary;
                            }
                            if (primary.isOnError()) {
                                return primary;
                            }
                            if (secondary.isOnError()) {
                                return secondary;
                            }
                            
                            return Notification.createOnNext(primary.getValue() && secondary.getValue());
                        }
                    }
                )
                .<Boolean>dematerialize()
                .subscribe(s);
            }
        });
    }
}
