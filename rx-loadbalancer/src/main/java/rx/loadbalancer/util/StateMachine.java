package rx.loadbalancer.util;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Subscriber;
import rx.functions.Action1;
import rx.functions.Action2;
import rx.functions.Func1;
import rx.subjects.PublishSubject;

public class StateMachine<T, E> implements Action1<E> {
    private static final Logger LOG = LoggerFactory.getLogger(StateMachine.class);
    
    public static class State<T, E> {
        private String name;
        private Func1<T, Observable<E>> enter;
        private Func1<T, Observable<E>> exit;
        private Map<E, State<T, E>> transitions = new HashMap<E, State<T, E>>();
        private Set<E> ignore = new HashSet<E>();
        
        public static <T, E> State<T, E> create(String name) {
            return new State<T, E>(name);
        }
        
        public State(String name) {
            this.name = name;
        }
        
        public State<T, E> onEnter(Func1<T, Observable<E>> func) {
            this.enter = func;
            return this;
        }
        
        public State<T, E> onExit(Func1<T, Observable<E>> func) {
            this.exit = func;
            return this;
        }
        
        public State<T, E> transition(E event, State<T, E> state) {
            transitions.put(event, state);
            return this;
        }
        
        public State<T, E> ignore(E event) {
            ignore.add(event);
            return this;
        }
        
        Observable<E> enter(T context) {
            if (enter != null)
                return enter.call(context);
            return Observable.empty();
        }
        
        Observable<E> exit(T context) {
            if (exit != null)
                exit.call(context);
            return Observable.empty();
        }
        
        State<T, E> next(E event) {
            return transitions.get(event);
        }
        
        public String toString() {
            return name;
        }
    }
    
    private volatile State<T, E> state;
    private final T context;
    private final PublishSubject<E> events = PublishSubject.create();
    
    public static <T, E> StateMachine<T, E> create(T context, State<T, E> initial) {
        return new StateMachine<T, E>(context, initial);
    }
    
    public StateMachine(T context, State<T, E> initial) {
        this.state = initial;
        this.context = context;
    }

    public Observable<Void> start() {
        return Observable.create(new OnSubscribe<Void>() {
            @Override
            public void call(Subscriber<? super Void> sub) {
                sub.add(events.collect(context, new Action2<T, E>() {
                        @Override
                        public void call(T context, E event) {
                            LOG.info("{} : {}({})", context, state, event);
                            final State<T, E> next = state.next(event);
                            if (next != null) {
                                state.exit(context);
                                state = next;
                                next.enter(context).subscribe(StateMachine.this);
                            }
                            else if (!state.ignore.contains(event)) {
                                LOG.info("Unexpected event {} in state {} for {} ", event, state, context);
                            }
                        }
                    })
                    .subscribe());
                
                state.enter(context);
            }
        });
    }
    
    @Override
    public void call(E event) {
        events.onNext(event);
    }
    
    public State<T, E> getState() {
        return state;
    }
    
    public T getContext() {
        return context;
    }

}
