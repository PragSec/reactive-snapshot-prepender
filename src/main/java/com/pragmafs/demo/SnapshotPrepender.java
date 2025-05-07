package com.pragmafs.demo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

// TODO: - think through error handling, is tryEmitNext the right thing?
/**
 * Constructs a snapshot prepender from two fluxes: the snapshot to be prepended
 * and the never-ending stream of updates to follow. It's called "prepender"
 * to emphasize that the updates are subscribed first but the snapshot emitted
 * first, followed by the updates. The purpose is to ensure there are no
 * missed elements from the hot update stream after snapshot state but before
 * the start of the updates.
 *
 * <p>Threading:
 *
 * <p>The updates subscribes (and is assumed to publish) on the default/immediate scheduler
 * of the subscriber (who calls subscribe on the flux returned from onFlux()).
 *
 * <p>The snapshot is subscribed and publishes on a new Scheduler.parallel() (TODO: provide scheduler to use in the constructor?)
 * This is a short-lived process that emits the snapshot, followed by the updates emitted and buffered
 * during the snapshot.
 *
 * TODO - test out what .publishOn and .subscribeOn do when added to the output flux - if they don't propagate
 *   up is it adequate?
 *
 * <p>After the snapshot and buffered rows are emitted,
 */
public class SnapshotPrepender<T> {

    private static final Logger log = LoggerFactory.getLogger(SnapshotPrepender.class);
    private final Flux<T> snapshot;
    private final Flux<T> updates;
    private final Sinks.Many<T> sink;
    private volatile Deque<T> buffer = new ArrayDeque<>();
    private final Lock lock = new ReentrantLock();
    private Disposable updateSubscription;
    private Disposable snapshotSubscription;

    public SnapshotPrepender(Flux<T> snapshot, Flux<T> updates) {
        this.snapshot = snapshot;
        this.updates = updates;
        // this.sink = Sinks.unsafe().many().unicast().onBackpressureError();
        this.sink = Sinks.many().unicast().onBackpressureError();
    }

    /**
     * Returns the combined flux containing the snapshot followed by
     * never-ending stream of updates.
     */
    Flux<T> asFlux() {
        return sink.asFlux()
            .doOnSubscribe(sub -> subscribe())
            .doOnCancel(() -> {
                if (updateSubscription != null) {
                    updateSubscription.dispose();
                }
                if (snapshotSubscription != null) {
                    snapshotSubscription.dispose();
                }
            });
    }

    public void subscribe() {
        updateSubscription = updates
            .doOnSubscribe(x -> log.info("Updates subscribed"))
            .subscribe(t -> {
                lock.lock();
                try {
                    if (buffer == null) {
                        emitUpdateMaybe(t);
                    } else {
                        buffer.add(t);
                    }
                } finally {
                    lock.unlock();
                }
            });

        snapshotSubscription = snapshot
            .doOnSubscribe(x -> log.info("Snapshot subscribed"))
            .doOnComplete(this::flushBuffer)
            .subscribeOn(Schedulers.newSingle("snapshot"))
            .subscribe(this::emitSnapshotMaybe);
    }

    private void flushBuffer() {
        int n = 0;
        log.info("Snapshot completed");
        lock.lock();
        try {
            T t;
            while ((t = buffer.poll()) != null) {
                emitUpdateMaybe(t);
                n++;
            }
            buffer = null;
        } finally {
            lock.unlock();
        }
        log.info("Buffer of {} flushed", n);
    }

    /**
     * Emits a snapshot item, maybe. This is a hook for creating
     * filtering behavior through overriding in a subclass. Base class behavior
     * is always to emit.
     */
    protected void emitSnapshotMaybe(T t) {
        tryEmitNext(t);
        // log.info("Snapshot emit {}", t);
    }

    /**
     * Emits an update item, maybe. This is a hook for creating
     * filtering behavior through overriding in a subclass. Base class behavior
     * is always to emit.
     */
    protected void emitUpdateMaybe(T t) {
        tryEmitNext(t);
    }

    /**
     * Emits an item to the subscriber, for use by derived classes.
     */
    protected void tryEmitNext(T t) {
        sink.tryEmitNext(t);
    }

}
