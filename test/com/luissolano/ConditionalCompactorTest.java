package com.luissolano;


import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
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

    @Test
    public void testBackpressure() {

        TestSubscriber<String> subscriber = new TestSubscriber<>(0);

        Observable<String> source = Observable.just("A", "A", "R", "S", "A", "R", "F", "R", "A", "A");

        source.toFlowable(BackpressureStrategy.BUFFER).lift(new Main.ConditionalCompactor(500, TimeUnit.HOURS, Schedulers.computation()))
                .subscribe(subscriber);

        subscriber.request(4);

        subscriber.assertValues("A", "A", "R", "M");
    }

    @Test
    public void testBackpressure2() {

        TestSubscriber<String> subscriber = new TestSubscriber<>(0);

        Observable<String> source = Observable.just("A", "A", "R", "S", "A", "R", "F", "R", "A", "A");

        source.toFlowable(BackpressureStrategy.BUFFER).lift(new Main.ConditionalCompactor(500, TimeUnit.HOURS, Schedulers.computation()))
                .subscribe(subscriber);

        subscriber.request(5);

        subscriber.assertValues("A", "A", "R", "M", "R");
    }

    @Test
    public void testBackpressure3() {

        TestSubscriber<String> subscriber = new TestSubscriber<>(0);

        Observable<String> source = Observable.just("A", "S", "A", "F", "A", "S", "A", "R", "F", "R");

        source.toFlowable(BackpressureStrategy.BUFFER).lift(new Main.ConditionalCompactor(500, TimeUnit.HOURS, Schedulers.computation()))
                .subscribe(subscriber);

        subscriber.request(1);

        subscriber.assertValues("A");

        subscriber.request(1);

        subscriber.assertValues("A", "M");

        subscriber.request(1);

        subscriber.assertValues("A", "M", "A");

        subscriber.request(1);

        subscriber.assertValues("A", "M", "A", "M");

        subscriber.request(1);

        subscriber.assertValues("A", "M", "A", "M", "R");

        subscriber.request(1);

        subscriber.assertValues("A", "M", "A", "M", "R");
        subscriber.assertComplete();

    }

    @Test
    public void testItFlushesOnBrokenWindowAndBackpressure() {

        TestSubscriber<String> subscriber = new TestSubscriber<>(0);

        Observable<String> source = Observable.just("A", "S", "A", "A", "S", "A", "R", "F", "R");

        source.toFlowable(BackpressureStrategy.BUFFER).lift(new Main.ConditionalCompactor(500, TimeUnit.HOURS, Schedulers.computation()))
                .subscribe(subscriber);

        subscriber.request(1);

        subscriber.assertValues("A");

        subscriber.request(1);

        subscriber.assertValues("A", "S");

        subscriber.request(1);

        subscriber.assertValues("A", "S", "A");

        subscriber.request(1);

        subscriber.assertValues("A", "S", "A", "A");

        subscriber.request(1);

        subscriber.assertValues("A", "S", "A", "A", "M");

        subscriber.request(1);

        subscriber.assertValues("A", "S", "A", "A", "M", "R");
        subscriber.assertComplete();
    }

    @Test
    public void testItFlushesOnTimeoutAndBackpressure() throws InterruptedException {

        TestSubscriber<String> subscriber = new TestSubscriber<>(0);

        Flowable<String> source = Flowable.concat(
                Flowable.just("A", "A", "R", "S", "A"),
                Flowable.just("R").delay(1, TimeUnit.SECONDS),
                Flowable.just("F", "R")
        );

        source.lift(new Main.ConditionalCompactor(100, TimeUnit.MILLISECONDS, Schedulers.computation()))
                .subscribe(subscriber);

        subscriber.request(1);

        subscriber.assertValues("A");

        subscriber.request(1);

        subscriber.assertValues("A", "A");

        subscriber.request(1);

        subscriber.assertValues("A", "A", "R");

        subscriber.request(1);

        Thread.sleep(600);

        subscriber.assertValues("A", "A", "R", "S");

        Thread.sleep(600);

        subscriber.assertValues("A", "A", "R", "S");

        subscriber.request(1);

        subscriber.assertValues("A", "A", "R", "S", "A");

        subscriber.request(1);

        subscriber.assertValues("A", "A", "R", "S", "A", "R");

        subscriber.request(1);

        subscriber.assertValues("A", "A", "R", "S", "A", "R", "F");

        subscriber.request(1);

        subscriber.assertValues("A", "A", "R", "S", "A", "R", "F", "R");

        subscriber.assertComplete();
    }

    


}
