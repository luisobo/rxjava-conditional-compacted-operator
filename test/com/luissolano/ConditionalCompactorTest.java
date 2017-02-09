package com.luissolano;


import io.reactivex.Flowable;
import io.reactivex.schedulers.Schedulers;
import io.reactivex.subscribers.TestSubscriber;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class ConditionalCompactorTest  {
    private final NonThrowingPredicate<String> opensWindow = "S"::equals;
    private final NonThrowingPredicate<String> closesWindow = "F"::equals;
    private final NonThrowingFunction<List<String>, String> compact = s -> String.join("", s);

    private Flowable<String> applyOperator(Flowable<String> source) {

        return source.lift(new ConditionalCompactor<>(opensWindow, closesWindow, compact, 500, TimeUnit.MILLISECONDS, Schedulers.computation()));
    }


    @Test
    public void testItProperlyCompacts() {

        TestSubscriber<String> subscriber = new TestSubscriber<>();

        Flowable<String> source = Flowable.just("A", "A", "R", "S", "A", "R", "F", "R", "A", "A");

        applyOperator(source).subscribe(subscriber);

        subscriber.assertValues("A", "A", "R", "SARF", "R", "A", "A");

    }

    @Test
    public void testItFlushesOnBrokenWindow() {

        TestSubscriber<String> subscriber = new TestSubscriber<>();

        Flowable<String> source = Flowable.just("A", "R", "S", "A", "R", "S", "R", "A", "F", "A");

        applyOperator(source).subscribe(subscriber);

        subscriber.assertValues("A", "R", "S", "A", "R", "SRAF", "A");
    }


    @Test
    public void testItFlushesOnTimeout() throws InterruptedException {

        TestSubscriber<String> subscriber = new TestSubscriber<>();

        Flowable<String> source = Flowable.concat(Flowable.just("A", "A", "R", "S", "A"), Flowable.just("R", "F", "R", "A", "A").delay(1, TimeUnit.SECONDS));

        applyOperator(source).subscribe(subscriber);

        subscriber.await(2, TimeUnit.SECONDS);

        subscriber.assertValues("A", "A", "R", "S", "A", "R", "F", "R", "A", "A");
    }

    @Test
    public void testBackpressure() {

        TestSubscriber<String> subscriber = new TestSubscriber<>(0);

        Flowable<String> source = Flowable.just("A", "A", "R", "S", "A", "R", "F", "R", "A", "A");

        applyOperator(source).subscribe(subscriber);

        subscriber.request(4);

        subscriber.assertValues("A", "A", "R", "SARF");
    }

    @Test
    public void testBackpressure2() {

        TestSubscriber<String> subscriber = new TestSubscriber<>(0);

        Flowable<String> source = Flowable.just("A", "A", "R", "S", "A", "R", "F", "R", "A", "A");

        applyOperator(source).subscribe(subscriber);

        subscriber.request(5);

        subscriber.assertValues("A", "A", "R", "SARF", "R");
    }

    @Test
    public void testBackpressure3() {

        TestSubscriber<String> subscriber = new TestSubscriber<>(0);

        Flowable<String> source = Flowable.just("A", "S", "A", "F", "A", "S", "A", "R", "F", "R");

        applyOperator(source).subscribe(subscriber);

        subscriber.request(1);

        subscriber.assertValues("A");

        subscriber.request(1);

        subscriber.assertValues("A", "SAF");

        subscriber.request(1);

        subscriber.assertValues("A", "SAF", "A");

        subscriber.request(1);

        subscriber.assertValues("A", "SAF", "A", "SARF");

        subscriber.request(1);

        subscriber.assertValues("A", "SAF", "A", "SARF", "R");

        subscriber.request(1);

        subscriber.assertValues("A", "SAF", "A", "SARF", "R");
        subscriber.assertComplete();

    }

    @Test
    public void testItFlushesOnBrokenWindowAndBackpressure() {

        TestSubscriber<String> subscriber = new TestSubscriber<>(0);

        Flowable<String> source = Flowable.just("A", "S", "A", "A", "S", "A", "R", "F", "R");

        applyOperator(source).subscribe(subscriber);

        subscriber.request(1);

        subscriber.assertValues("A");

        subscriber.request(1);

        subscriber.assertValues("A", "S");

        subscriber.request(1);

        subscriber.assertValues("A", "S", "A");

        subscriber.request(1);

        subscriber.assertValues("A", "S", "A", "A");

        subscriber.request(1);

        subscriber.assertValues("A", "S", "A", "A", "SARF");

        subscriber.request(1);

        subscriber.assertValues("A", "S", "A", "A", "SARF", "R");
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

        applyOperator(source).subscribe(subscriber);

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
