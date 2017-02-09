package com.luissolano;

import io.reactivex.FlowableOperator;
import io.reactivex.Scheduler;
import io.reactivex.disposables.Disposable;
import io.reactivex.disposables.Disposables;
import io.reactivex.disposables.SerialDisposable;
import io.reactivex.internal.util.BackpressureHelper;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/* Imported from http://stackoverflow.com/questions/42114546/rxjava-substitute-a-subsequence-of-elements-with-a-single-element/42117531#42117531
    and later update from https://github.com/luisobo/rxjava-conditional-compacted-operator/pull/2#issuecomment-278732974
    author David Karnok (https://github.com/akarnokd)
 */
public final class ConditionalCompactor implements FlowableOperator<String, String> {
    private final Scheduler scheduler;

    private final long timeout;

    private final TimeUnit unit;

    public ConditionalCompactor(long timeout, TimeUnit unit,
                         Scheduler scheduler) {
        this.scheduler = scheduler;
        this.timeout = timeout;
        this.unit = unit;
    }

    @Override
    public Subscriber<? super String> apply(Subscriber<? super String> t) {
        return new ConditionalCompactorSubscriber(
                t, timeout, unit, scheduler.createWorker());
    }

    private static final class ConditionalCompactorSubscriber
            implements Subscriber<String>, Subscription {
        final Subscriber<? super String> actual;

        final Scheduler.Worker worker;

        final long timeout;

        final TimeUnit unit;

        final AtomicInteger wip;

        final SerialDisposable mas;

        final Queue<String> queue;

        final List<String> batch;

        final AtomicLong requested;

        Subscription s;

        static final Disposable NO_TIMER;

        static {
            NO_TIMER = Disposables.empty();
            NO_TIMER.dispose();
        }

        volatile boolean done;
        Throwable error;

        boolean compacting;

        int lastLength;

        private ConditionalCompactorSubscriber(Subscriber<? super String> actual,
                                       long timeout, TimeUnit unit, Scheduler.Worker worker) {
            this.actual = actual;
            this.worker = worker;
            this.timeout = timeout;
            this.unit = unit;
            this.batch = new ArrayList<>();
            this.wip = new AtomicInteger();
            this.mas = new SerialDisposable();
            this.mas.set(NO_TIMER);
            this.queue = new ConcurrentLinkedQueue<>();
            this.requested = new AtomicLong();
        }

        @Override
        public void onSubscribe(Subscription s) {
            this.s = s;
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

        @Override
        public void cancel() {
            s.cancel();
            worker.dispose();
        }

        @Override
        public void request(long n) {
            BackpressureHelper.add(requested, n);
            s.request(n);
            drain();
        }

        void drain() {
            if (wip.getAndIncrement() != 0) {
                return;
            }
            int missed = 1;
            for (; ; ) {

                long r = requested.get();
                long e = 0L;

                while (e != r) {
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
                            if (n > 1) {
                                actual.onNext(queue.poll());
                                mas.set(NO_TIMER);
                                lastLength = -1;
                                compacting = false;
                                e++;
                                continue;
                            }
                            // keep the last as the start of the new
                            if (lastLength <= 0) {
                                lastLength = 1;
                                mas.set(worker.schedule(() -> {
                                    queue.offer("T");
                                    drain();
                                }, timeout, unit));
                                this.s.request(1);
                            }
                            break;
                        } else if ("T".equals(last)) {
                            actual.onNext(queue.poll());
                            compacting = false;
                            mas.set(NO_TIMER);
                            lastLength = -1;
                            e++;
                            continue;
                        } else if ("F".equals(last)) {
                            actual.onNext("M");
                            while (n-- != 0) {
                                queue.poll();
                            }
                            compacting = false;
                            mas.set(NO_TIMER);
                            lastLength = -1;
                            e++;
                        } else {
                            if (lastLength != n) {
                                lastLength = n;
                                mas.set(worker.schedule(() -> {
                                    queue.offer("T");
                                    drain();
                                }, timeout, unit));
                                this.s.request(1);
                            }
                            break;
                        }
                    } else {
                        if ("A".equals(s) || "F".equals(s) || "R".equals(s)) {
                            queue.poll();
                            actual.onNext(s);
                            e++;
                        } else if ("T".equals(s)) {
                            // ignore timeout markers outside the compacting mode
                            queue.poll();
                        } else {
                            compacting = true;
                        }
                    }
                }

                if (e != 0L) {
                    BackpressureHelper.produced(requested, e);
                }

                if (e == r) {
                    if (done) {
                        if (error != null) {
                            queue.clear();
                            actual.onError(error);
                            worker.dispose();
                            return;
                        }
                        if (queue.isEmpty()) {
                            actual.onComplete();
                            worker.dispose();
                            return;
                        }
                    }
                }

                missed = wip.addAndGet(-missed);
                if (missed == 0) {
                    break;
                }
            }
        }
    }
}
