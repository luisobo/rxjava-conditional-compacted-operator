package com.luissolano;


import io.reactivex.BackpressureStrategy;
import io.reactivex.Observable;
import io.reactivex.schedulers.Schedulers;
import io.reactivex.subscribers.TestSubscriber;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class ConditionalCompactorTest  {


    @Test
    public void testItProperlyCompacts() {

        TestSubscriber<String> subscriber = new TestSubscriber<>();

        Observable<String> source = Observable.just("A", "A", "R", "S", "A", "R", "F", "R", "A", "A");

        source.toFlowable(BackpressureStrategy.BUFFER).lift(new Main.ConditionalCompactor(500, TimeUnit.MILLISECONDS, Schedulers.computation()))
                .subscribe(subscriber);

        subscriber.assertValues("A", "A", "R", "M", "R", "A", "A");

    }

    @Test
    public void testItFlushesOnBrokenWindow() {

        TestSubscriber<String> subscriber = new TestSubscriber<>();

        Observable<String> source = Observable.just("A", "R", "S", "A", "R", "S", "R", "A", "F", "A");

        source.toFlowable(BackpressureStrategy.BUFFER).lift(new Main.ConditionalCompactor(500, TimeUnit.MILLISECONDS, Schedulers.computation()))
                .subscribe(subscriber);

        subscriber.assertValues("A", "R", "S", "A", "R", "M", "A");
    }


    @Test
    public void testItFlushesOnTimeout() throws InterruptedException {

        TestSubscriber<String> subscriber = new TestSubscriber<>();

        Observable<String> source = Observable.concat(Observable.just("A", "A", "R", "S", "A"), Observable.just("R", "F", "R", "A", "A").delay(1, TimeUnit.SECONDS));

        source.toFlowable(BackpressureStrategy.BUFFER).lift(new Main.ConditionalCompactor(500, TimeUnit.MILLISECONDS, Schedulers.computation()))
                .subscribe(subscriber);



        subscriber.await(2, TimeUnit.SECONDS);

        subscriber.assertValues("A", "A", "R", "S", "A", "R", "F", "R", "A", "A");
    }

    


}
