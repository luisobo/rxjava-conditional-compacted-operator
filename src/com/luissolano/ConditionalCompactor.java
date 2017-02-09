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
public final class ConditionalCompactor<T> implements FlowableOperator<T, T> {

    private final NonThrowingPredicate<T> opensWindow;
    private final NonThrowingPredicate<T> closesWindow;
    private final NonThrowingFunction<List<T>, T> compactor;

    private final Scheduler scheduler;

    private final long timeout;

    private final TimeUnit unit;

    public ConditionalCompactor(NonThrowingPredicate<T> opensWindow, NonThrowingPredicate<T> closesWindow, NonThrowingFunction<List<T>, T> compactor, long timeout, TimeUnit unit,
                         Scheduler scheduler) {
        this.opensWindow = opensWindow;
        this.closesWindow = closesWindow;
        this.compactor = compactor;
        this.scheduler = scheduler;
        this.timeout = timeout;
        this.unit = unit;
    }

    @Override
    public Subscriber<? super T> apply(Subscriber<? super T> t) {
        return new ConditionalCompactorSubscriber<>(
                t,
                opensWindow, closesWindow, compactor,
                timeout, unit, scheduler.createWorker());
    }

    private static final class ConditionalCompactorSubscriber<T>
            implements Subscriber<T>, Subscription {

        private final NonThrowingPredicate<T> opensWindow;
        private final NonThrowingPredicate<T> closesWindow;
        private final NonThrowingFunction<List<T>, T> compactor;

        final Subscriber<? super T> actual;

        final Scheduler.Worker worker;

        final long timeout;

        final TimeUnit unit;

        final AtomicInteger wip;

        final SerialDisposable mas;

        final Queue<T> queue;

        final List<T> batch;

        final AtomicLong requested;

        Subscription s;

        static final Disposable NO_TIMER;

        static final Object timeoutMarker;

        static {
            timeoutMarker = new Object();
            NO_TIMER = Disposables.empty();
            NO_TIMER.dispose();
        }

        volatile boolean done;
        Throwable error;

        boolean compacting;

        int lastLength;

        private ConditionalCompactorSubscriber(Subscriber<? super T> actual,
                                               NonThrowingPredicate<T> opensWindow, NonThrowingPredicate<T> closesWindow, NonThrowingFunction<List<T>, T> compactor,
                                       long timeout, TimeUnit unit, Scheduler.Worker worker) {
            this.opensWindow = opensWindow;
            this.closesWindow = closesWindow;
            this.compactor = compactor;
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
        public void onNext(T t) {
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
                    T s = queue.peek();
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
                        T last = batch.get(n - 1);
                        if (timeoutMarker.equals(last)) {
                            actual.onNext(queue.poll());
                            compacting = false;
                            mas.set(NO_TIMER);
                            lastLength = -1;
                            e++;
                            continue;
                        } else
                        if (opensWindow.test(last)) {
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
                                    queue.offer((T) timeoutMarker);
                                    drain();
                                }, timeout, unit));
                                this.s.request(1);
                            }
                            break;
                        } else if (closesWindow.test(last)) {
                            actual.onNext(compactor.apply(batch));
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
                                    queue.offer((T) timeoutMarker);
                                    drain();
                                }, timeout, unit));
                                this.s.request(1);
                            }
                            break;
                        }
                    } else {
                        if (timeoutMarker.equals(s)) {
                            // ignore timeout markers outside the compacting mode
                            queue.poll();
                        } else if (opensWindow.test(s)) {
                            compacting = true;
                        } else {
                            queue.poll();
                            actual.onNext(s);
                            e++;
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
