package netflix.ocelli;

import java.util.concurrent.TimeUnit;

import netflix.ocelli.functions.Connectors;
import netflix.ocelli.functions.Delays;
import netflix.ocelli.util.RxUtil;

import org.junit.Ignore;
import org.junit.Test;

import rx.Observable;
import rx.Subscription;
import rx.functions.Func1;
import rx.subjects.PublishSubject;

public class FailureDetectingInstanceTest {
    @Test
    @Ignore
    public void test() throws InterruptedException {
        
        final PublishSubject<Throwable> errorStream = PublishSubject.create();
        Func1<Integer, Observable<Void>> connector = new Func1<Integer, Observable<Void>>() {
            private volatile int counter = 3;
            
            @Override
            public Observable<Void> call(Integer count) {
                if (counter-- > 0) {
                    return Observable.error(new Exception("ConnectError-" + count));
                }
                else {
                    counter = 3;
                    return Observable.empty();
                }
            }
        };
        
        connector = Connectors.immediate();
        
        FailureDetectingInstance<Integer> instance = new FailureDetectingInstance<Integer>(1, connector, errorStream, Delays.fixed(1, TimeUnit.SECONDS));
        
        Subscription sub1 = instance
            .doOnNext(RxUtil.info("1 state"))
            .subscribe();
        
        Subscription sub2 = instance
                .doOnNext(RxUtil.info("2 state"))
                .subscribe();
//            
//        errorStream.onNext(new Throwable("Manual error"));
//        
//        Subscription sub3 = instance
//                .doOnNext(RxUtil.info("3 state"))
//                .subscribe();
//            
//        errorStream.onNext(new Throwable("Manual error"));
//        
//        sub1.unsubscribe();
//        sub2.unsubscribe();
//        sub3.unsubscribe();
//        
//        Observable.interval(1, TimeUnit.SECONDS).subscribe(new Action1<Long>() {
//            @Override
//            public void call(Long t1) {
//                errorStream.onNext(new Exception("Error-" + t1));
//            }
//        });
    }
}
