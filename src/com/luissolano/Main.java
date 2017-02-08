package com.luissolano;

import io.reactivex.BackpressureStrategy;
import io.reactivex.FlowableOperator;
import io.reactivex.Observable;
import io.reactivex.Scheduler;
import io.reactivex.disposables.Disposable;
import io.reactivex.disposables.SerialDisposable;
import io.reactivex.internal.disposables.EmptyDisposable;
import io.reactivex.schedulers.Schedulers;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class Main {

    public static void main(String[] args) {
        Observable<String> source = Observable.just("A", "A", "R", "S", "A", "R", "F", "R", "A", "A");



        source.toFlowable(BackpressureStrategy.BUFFER).lift(new ConditionalCompactor(500, TimeUnit.SECONDS, Schedulers.computation()))
                .subscribe(System.out::println, Throwable::printStackTrace);

    }

    static final class ConditionalCompactor implements FlowableOperator<String, String> {
        final Scheduler scheduler;

        final long timeout;

        final TimeUnit unit;

        ConditionalCompactor(long timeout, TimeUnit unit, Scheduler scheduler) {
            this.scheduler = scheduler;
            this.timeout = timeout;
            this.unit = unit;
        }

        @Override
        public Subscriber<? super String> apply(Subscriber<? super String> t) {
            ConditionalCompactorSubscriber parent = new ConditionalCompactorSubscriber(t, timeout, unit, scheduler.createWorker());

//            t.add(parent);
//            t.add(parent.worker);
            // t.setProducer(parent.requested);

            return parent;
        }

        static final class ConditionalCompactorSubscriber implements Subscription, Subscriber<String> {
            final Subscriber<? super String> actual;

            Subscription s;

            final Scheduler.Worker worker;

            final long timeout;

            final TimeUnit unit;

            final AtomicInteger wip;

            final SerialDisposable mas;

            final Queue<String> queue;

            final List<String> batch;

            static final Disposable NO_TIMER;
            static {
                NO_TIMER = EmptyDisposable.INSTANCE;
            }

            volatile boolean done;
            Throwable error;

            boolean compacting;

            int lastLength;

            ConditionalCompactorSubscriber(Subscriber<? super String> actual, long timeout, TimeUnit unit, Scheduler.Worker worker) {
                this.actual = actual;
                this.worker = worker;
                this.timeout = timeout;
                this.unit = unit;
                this.batch = new ArrayList<>();
                this.wip = new AtomicInteger();
                this.mas = new SerialDisposable();
                this.mas.set(NO_TIMER);
                this.queue = new ConcurrentLinkedQueue<>();
            }

            @Override
            public void onSubscribe(Subscription subscription) {
                this.s = subscription;
                actual.onSubscribe(this);
            }

            @Override
            public void onNext(String t) {
                queue.offer(t);
                drain();
            }

            @Override
            public void onError(Throwable e) {
                error = e;
                done = true;
                drain();
            }

            @Override
            public void onComplete() {
                done = true;
                drain();
            }

            void drain() {
                if (wip.getAndIncrement() != 0) {
                    return;
                }
                int missed = 1;
                for (;;) {

                    for (;;) {
                        boolean d = done;
                        if (d && error != null) {
                            queue.clear();
                            actual.onError(error);
                            worker.dispose();
                            return;
                        }
                        String s = queue.peek();
                        if (s == null) {
                            if (d) {
                                actual.onComplete();
                                worker.dispose();
                                return;
                            }
                            break;
                        }

                        if (compacting) {
                            batch.clear();
                            batch.addAll(queue);
                            int n = batch.size();
                            String last = batch.get(n - 1);
                            if ("S".equals(last)) {
                                while (--n != 0) {
                                    actual.onNext(queue.poll());
                                }
                                // keep the last as the start of the new
                                if (lastLength <= 0) {
                                    lastLength = 1;
                                    mas.set(worker.schedule(() -> {
                                        queue.offer("T");
                                        drain();
                                    }, timeout, unit));
                                }
                                break;
                            } else
                            if ("T".equals(last)) {
                                while (--n != 0) {
                                    actual.onNext(queue.poll());
                                }
                                queue.poll(); // pop timeout marker
                                compacting = false;
                                mas.set(NO_TIMER);
                                lastLength = -1;
                                continue;
                            } else
                            if ("F".equals(last)) {
                                actual.onNext("M");
                                while (n-- != 0) {
                                    queue.poll();
                                }
                                compacting = false;
                                mas.set(NO_TIMER);
                                lastLength = -1;
                                continue;
                            } else {
                                if (lastLength != n) {
                                    lastLength = n;
                                    mas.set(worker.schedule(() -> {
                                        queue.offer("T");
                                        drain();
                                    }, timeout, unit));
                                }
                                break;
                            }
                        } else {
                            if ("A".equals(s) || "F".equals(s) || "R".equals(s)) {
                                queue.poll();
                                actual.onNext(s);
                                continue;
                            } else
                            if ("T".equals(s)) {
                                queue.poll(); // ignore timeout markers outside the compacting mode
                            } else {
                                compacting = true;
                                continue;
                            }
                        }
                    }

                    missed = wip.addAndGet(-missed);
                    if (missed == 0) {
                        break;
                    }
                }
            }

            @Override
            public void request(long l) {
                s.request(l);
            }

            @Override
            public void cancel() {
                s.cancel();
            }
        }
    }
}
