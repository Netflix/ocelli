package netflix.ocelli.util;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Observable.Operator;
import rx.Observer;
import rx.Subscriber;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.functions.Func0;
import rx.functions.Func1;
import rx.functions.FuncN;
import rx.subscriptions.Subscriptions;

public class RxUtil {
    private static final Logger LOG = LoggerFactory.getLogger(RxUtil.class);
    
    private interface Action01<T> extends Action1<T>, Action0 {}
    
    /**
     * Increment a stateful counter outside the stream.
     * 
     * {code
     * <pre>
     * observable
     *    .doOnNext(RxUtil.increment(mycounter))
     * </pre>
     * }
     *  
     * @param metric
     * @return
     */
    public static <T> Action01<T> increment(final AtomicLong metric) {
        return new Action01<T>() {
            @Override
            public void call(T t1) {
                metric.incrementAndGet();
            }

            @Override
            public void call() {
                metric.incrementAndGet();
            }
        };
    }
    
    /**
     * Decrement a stateful counter outside the stream.
     * 
     * {code
     * <pre>
     * observable
     *    .doOnNext(RxUtil.decrement(mycounter))
     * </pre>
     * }
     *  
     * @param metric
     * @return
     */
    public static <T> Action01<T> decrement(final AtomicLong metric) {
        return new Action01<T>() {
            @Override
            public void call(T t1) {
                metric.decrementAndGet();
            }

            @Override
            public void call() {
                metric.decrementAndGet();
            }
        };
    }
    
    /**
     * Trace each item emitted on the stream with a given label.  
     * Will log the file and line where the trace occurs.
     * 
     * {code
     * <pre>
     * observable
     *    .doOnNext(RxUtil.trace("next: "))
     * </pre>
     * }
     *  
     * @param label
     */
    public static <T> Action01<T> trace(String label) {
        final String caption = getSourceLabel(label);
        final AtomicLong counter = new AtomicLong();
        return new Action01<T>() {
            @Override
            public void call(T t1) {
                LOG.trace("{} ({}) {}", caption, counter.incrementAndGet(), t1);
            }

            @Override
            public void call() {
                LOG.trace("{} ({}) {}", caption, counter.incrementAndGet());
            }
        };            
    }

    /**
     * Log info line for each item emitted on the stream with a given label.  
     * Will log the file and line where the trace occurs.
     * 
     * {code
     * <pre>
     * observable
     *    .doOnNext(RxUtil.info("next: "))
     * </pre>
     * }
     *  
     * @param label
     */
    public static <T> Action01<T> info(String label) {
        final String caption = getSourceLabel(label);
        final AtomicLong counter = new AtomicLong();
        return new Action01<T>() {
            @Override
            public void call(T t1) {
                LOG.info("{} ({}) {}", caption, counter.incrementAndGet(), t1);
            }

            @Override
            public void call() {
                LOG.info("{} ({})", caption, counter.incrementAndGet());
            }
        };            
    }
    
    /**
     * Action to sleep in the middle of a pipeline.  This is normally used in tests to
     * introduce an artifical delay.
     * @param timeout
     * @param units
     * @return
     */
    public static <T> Action01<T> sleep(final long timeout, final TimeUnit units) {
        return new Action01<T>() {
            @Override
            public void call(T t1) {
                call();
            }

            @Override
            public void call() {
                try {
                    units.sleep(timeout);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }
        };            
    }
    
    /**
     * Decrement a countdown latch for each item
     * 
     * {code
     * <pre>
     * observable
     *    .doOnNext(RxUtil.decrement(latch))
     * </pre>
     * }
     */
    public static <T> Action01<T> countdown(final CountDownLatch latch) {
        return new Action01<T>() {
            @Override
            public void call(T t1) {
                latch.countDown();
            }

            @Override
            public void call() {
                latch.countDown();
            }
        };            
    }

    /**
     * Log the request rate at the given interval.
     * 
     * {code
     * <pre>
     * observable
     *    .lift(RxUtil.rate("items per 10 seconds", 10, TimeUnit.SECONDS))
     * </pre>
     * }
     *  
     * @param label
     * @param interval 
     * @param units
     */
    public static String[] TIME_UNIT = {"ns", "us", "ms", "s", "m", "h", "d"};
    
    public static <T> Operator<T, T> rate(final String label, final long interval, final TimeUnit units) {
        final String caption = getSourceLabel(label);
        return new Operator<T, T>() {
            @Override
            public Subscriber<? super T> call(final Subscriber<? super T> child) {
                final AtomicLong counter = new AtomicLong();
                final String sUnits = (interval == 1) ? TIME_UNIT[units.ordinal()] : String.format("({} {})", interval, TIME_UNIT[units.ordinal()]);
                child.add(
                    Observable.interval(interval, units)
                    .subscribe(new Action1<Long>() {
                        @Override
                        public void call(Long t1) {
                            LOG.info("{} {} / {}", caption, counter.getAndSet(0), sUnits);
                        }
                    }));
                return new Subscriber<T>(child) {
                    @Override
                    public void onCompleted() {
                        if (!isUnsubscribed()) 
                            child.onCompleted();
                    }

                    @Override
                    public void onError(Throwable e) {
                        if (!isUnsubscribed()) 
                            child.onError(e);
                    }

                    @Override
                    public void onNext(T t) {
                        counter.incrementAndGet();
                        if (!isUnsubscribed()) 
                            child.onNext(t);
                    }
                };
            }
        };            
    }

    /**
     * Log error line when an error occurs.
     * Will log the file and line where the trace occurs.
     * 
     * {code
     * <pre>
     * observable
     *    .doOnError(RxUtil.error("Stream broke"))
     * </pre>
     * }
     *  
     * @param label
     */
    public static Action1<Throwable> error(String label) {
        final String caption = getSourceLabel(label);
        final AtomicLong counter = new AtomicLong();
        return new Action1<Throwable>() {
            @Override
            public void call(Throwable t1) {
                LOG.error("{} ({}) {}", caption, counter.incrementAndGet(), t1);
            }
        };            
    }

    /**
     * Log a warning line when an error occurs.
     * Will log the file and line where the trace occurs.
     * 
     * {code
     * <pre>
     * observable
     *    .doOnError(RxUtil.warn("Stream broke"))
     * </pre>
     * }
     *  
     * @param label
     */
    public static Action1<Throwable> warn(String label) {
        final String caption = getSourceLabel(label);
        final AtomicLong counter = new AtomicLong();
        return new Action1<Throwable>() {
            @Override
            public void call(Throwable t1) {
                LOG.warn("{} ({}) {}", caption, counter.incrementAndGet(), t1);
            }
        };            
    }

    public static <T> Func1<List<T>, Boolean> listNotEmpty() {
        return new Func1<List<T>, Boolean>() {
            @Override
            public Boolean call(List<T> t1) {
                return !t1.isEmpty();
            }
        };
    }

    /**
     * Filter out any collection that is empty.
     * 
     * {code
     * <pre>
     * observable
     *    .filter(RxUtil.collectionNotEmpty())
     * </pre>
     * }
     */
    public static <T> Func1<Collection<T>, Boolean> collectionNotEmpty() {
        return new Func1<Collection<T>, Boolean>() {
            @Override
            public Boolean call(Collection<T> t1) {
                return !t1.isEmpty();
            }
        };
    }

    /**
     * Operator that acts as a pass through.  Use this when you want the operator
     * to be interchangable with the default implementation being a single passthrough.
     * 
     * {code
     * <pre>
     * Operator<T,T> customOperator = RxUtil.passthrough();
     * observable
     *    .lift(customOperator)
     * </pre>
     * }
     */
    public static <T> Operator<T, T> passthrough() {
        return new Operator<T, T>() {
            @Override
            public Subscriber<? super T> call(final Subscriber<? super T> o) {
                return o;
            }
        };
    }
    
    /**
     * Cache all items and emit a single LinkedHashSet with all data when onComplete is called
     * @return
     */
    public static <T> Operator<Set<T>, T> toLinkedHashSet() {
        return new Operator<Set<T>, T>() {
            @Override
            public Subscriber<? super T> call(final Subscriber<? super Set<T>> o) {
                final Set<T> set = new LinkedHashSet<T>();
                return new Subscriber<T>() {
                    @Override
                    public void onCompleted() {
                        o.onNext(set);
                        o.onCompleted();
                    }

                    @Override
                    public void onError(Throwable e) {
                        o.onError(e);
                    }

                    @Override
                    public void onNext(T t) {
                        set.add(t);
                    }
                };
            }
        };
    }
    
    private static String getSourceLabel(String label) {
        StackTraceElement[] stack = Thread.currentThread().getStackTrace();
        StackTraceElement element = stack[3];
        return "(" + element.getFileName() + ":" + element.getLineNumber() + ") " + label;
    }

    /**
     * Filter that returns true whenever an external state is true
     * {code
     * <pre>
     * final AtomicBoolean condition = new AtomicBoolean();
     * 
     * observable
     *    .filter(RxUtil.isTrue(condition))
     * </pre>
     * }
     * @param condition
     */
    public static <T> Func1<T, Boolean> isTrue(final AtomicBoolean condition) {
        return new Func1<T, Boolean>() {
            @Override
            public Boolean call(T t1) {
                return condition.get();
            }
        };
    }

    /**
     * Filter that returns true whenever an external state is false
     * {code
     * <pre>
     * final AtomicBoolean condition = new AtomicBoolean();
     * 
     * observable
     *    .filter(RxUtil.isTrue(condition))
     * </pre>
     * }
     * @param condition
     */
    public static <T> Func1<T, Boolean> isFalse(final AtomicBoolean condition) {
        return new Func1<T, Boolean>() {
            @Override
            public Boolean call(T t1) {
                return !condition.get();
            }
        };
    }

    /**
     * Filter that returns true whenever a CAS operation on an external static 
     * AtomicBoolean succeeds 
     * {code
     * <pre>
     * final AtomicBoolean condition = new AtomicBoolean();
     * 
     * observable
     *    .filter(RxUtil.isTrue(condition))
     * </pre>
     * }
     * @param condition
     */
    public static Func1<? super Long, Boolean> compareAndSet(final AtomicBoolean condition, final boolean expect, final boolean value) {
        return new Func1<Long, Boolean>() {
            @Override
            public Boolean call(Long t1) {
                return condition.compareAndSet(expect, value);
            }
        };
    }

    /**
     * Simple operation that sets an external condition for each emitted item
     * {code
     * <pre>
     * final AtomicBoolean condition = new AtomicBoolean();
     * 
     * observable
     *    .doOnNext(RxUtil.set(condition, true))
     * </pre>
     * }
     * @param condition
     * @param value
     * @return
     */
    public static <T> Action01<T> set(final AtomicBoolean condition, final boolean value) {
        return new Action01<T>() {
            @Override
            public void call(T t1) {
                condition.set(value);
            }

            @Override
            public void call() {
                condition.set(value);
            }
        };
    }
    
    /**
     * Filter that always returns a constant value.  Use this to create a default filter
     * when the filter implementation is plugable. 
     * 
     * @param constant
     * @return
     */
    public static <T> Func1<T, Boolean> constantFilter(final boolean constant) {
        return new Func1<T, Boolean>() {
            @Override
            public Boolean call(T t1) {
                return constant;
            }
        };
    }

    /**
     * Observable factory to be used with {@link Observable.defer()} which will round robin
     * through a list of {@link Observable}'s so that each subscribe() returns the next
     * {@link Observable} in the list.  
     * 
     * @param sources
     * @return
     */
    public static <T> Func0<Observable<T>> roundRobinObservableFactory(@SuppressWarnings("unchecked") final Observable<T> ... sources) {
        return new Func0<Observable<T>>() {
            final AtomicInteger count = new AtomicInteger();
            @Override
            public Observable<T> call() {
                int index = count.getAndIncrement() % sources.length;
                return sources[index];
            }
        };
    }
    
    public static <T> Observable<Observable<T>> onSubscribeChooseNext(final Observable<T> ... sources) {
        return Observable.create(new OnSubscribe<Observable<T>>() {
            private AtomicInteger count = new AtomicInteger();
            
            @Override
            public void call(Subscriber<? super Observable<T>> t1) {
                int index = count.getAndIncrement();
                if (index < sources.length) {
                    t1.onNext(sources[index]);
                }
                t1.onCompleted();
            }
        });
    }

    /**
     * Given a list of observables that emit a boolean condition AND all conditions whenever
     * any condition changes and emit the resulting condition when the final condition changes.
     * @param sources
     * @return
     */
    public static Observable<Boolean> conditionAnder(List<Observable<Boolean>> sources) {
        return Observable.combineLatest(sources, new FuncN<Observable<Boolean>>() {
            @Override
            public Observable<Boolean> call(Object... args) {
                return Observable.from(args).cast(Boolean.class).firstOrDefault(true, new Func1<Boolean, Boolean>() {
                    @Override
                    public Boolean call(Boolean status) {
                        return !status;
                    }
                });
            }
        })
        .flatMap(new Func1<Observable<Boolean>, Observable<Boolean>>() {
            @Override
            public Observable<Boolean> call(Observable<Boolean> t1) {
                return t1;
            }
        })
        .distinctUntilChanged();
    }

    /**
     * Trace all parts of an observable's state and especially when 
     * notifications are discarded due to being unsubscribed.  This should
     * only used for debugging purposes.
     * @param label
     * @return
     */
    public static <T> Operator<T, T> uberTracer(String label) {
        final String caption = getSourceLabel(label);
        return new Operator<T, T>() {
            @Override
            public Subscriber<? super T> call(final Subscriber<? super T> s) {
                s.add(Subscriptions.create(new Action0() {
                    @Override
                    public void call() {
                        LOG.info("{} unsubscribing", caption);
                    }
                }));
                return new Subscriber<T>(s) {
                    private AtomicLong completedCounter = new AtomicLong();
                    private AtomicLong nextCounter = new AtomicLong();
                    private AtomicLong errorCounter = new AtomicLong();
                    
                    @Override
                    public void onCompleted() {
                        if (!s.isUnsubscribed()) {
                            s.onCompleted();
                        }
                        else {
                            LOG.info("{} ({}) Discarding onCompleted", caption, completedCounter.incrementAndGet());
                        }
                    }

                    @Override
                    public void onError(Throwable e) {
                        if (!s.isUnsubscribed()) {
                            s.onCompleted();
                        }
                        else {
                            LOG.info("{} ({}) Discarding onError", caption, errorCounter.incrementAndGet());
                        }
                    }

                    @Override
                    public void onNext(T t) {
                        if (!s.isUnsubscribed()) {
                            s.onNext(t);
                        }
                        else {
                            LOG.info("{} ({}) Discarding onNext", caption, nextCounter.incrementAndGet());
                        }
                    }
                };
            }
        };
    }

    /**
     * Utility to call an action when any event occurs regardless that event
     * @param action
     * @return
     */
    public static <T> Observer<T> onAny(final Action0 action) {
        return new Observer<T>() {

            @Override
            public void onCompleted() {
                action.call();
            }

            @Override
            public void onError(Throwable e) {
                action.call();
            }

            @Override
            public void onNext(T t) {
                action.call();
            }
        } ;
    }
    
    public static <T> Action01<T> acquire(final Semaphore sem) {
        return new Action01<T>() {
            @Override
            public void call(T t1) {
                call();
            }

            @Override
            public void call() {
                try {
                    sem.acquire();
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        };
    }
    
    public static <T> Action01<T> release(final Semaphore sem) {
        return new Action01<T>() {
            @Override
            public void call(T t1) {
                call();
            }

            @Override
            public void call() {
                sem.release();
            }
        };
    }
}
