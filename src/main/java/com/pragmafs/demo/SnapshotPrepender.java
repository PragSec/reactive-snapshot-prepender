package com.pragmafs.demo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.lang.NonNull;
import reactor.core.Disposable;
import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;

/**
 * SnapshotPrepender is a utility class that allows you to prepend a snapshot of items to a stream of updates.
 * <p>
 * It does this by taking a snapshot of the items in the snapshot stream and then concatenating it with the
 * hot updates stream in 3 distinct phases:
 * <p>
 * The snapshot phase takes items from the cold snapshot stream.
 * The buffered updates phase TAKES items from the updates stream UNTIL the snapshot phase is completed.
 * The live updates phase SKIPS items from the updates stream UNTIL the snapshot phase is completed.
 * <p>
 * Concatenating the three phases produces the final stream.
 *
 * @param <T>
 */
public class SnapshotPrepender<T> {
    public enum BackpressureStrategy {
        ERROR,
        BUFFER,
        DROP,
        LATEST
    }

    public static class Builder<T> {
        private Flux<T> snapshot;
        private Flux<T> updates;
        private BackpressureStrategy backpressureStrategy = BackpressureStrategy.BUFFER;
        private boolean skipIfSeenInSnapshot = false;
        private Predicate<T> snapshotEventFilter = t -> true;
        private Predicate<T> updateEventFilter = t -> true;

        /**
         * Sets the snapshot stream. This is a cold stream that will be connected to the updates stream.
         *
         * @param snapshot Snapshot stream. This is a cold stream that will be streamed before the updates stream.
         * @return this
         */
        public Builder<T> snapshot(Flux<T> snapshot) {
            this.snapshot = snapshot;
            return this;
        }

        /**
         * Updates stream to prepend to the snapshot.
         *
         * @param updates Updates stream. This is a hot stream that will be streamed after the snapshot stream.
         * @return this
         */
        public Builder<T> updates(Flux<T> updates) {
            this.updates = updates;
            return this;
        }

        /**
         * Skip items that were seen in the snapshot phase.
         *
         * @param skipIfSeenInSnapshot If true, skip update stream items that were seen in the snapshot phase. Default is false.
         * @return this
         */
        public Builder<T> skipIfSeenInSnapshot(boolean skipIfSeenInSnapshot) {
            this.skipIfSeenInSnapshot = skipIfSeenInSnapshot;
            return this;
        }

        /**
         * Sets the backpressure strategy.
         *
         * @param strategy Backpressure strategy. Default is BUFFER.
         * @return this
         */
        public Builder<T> backpressure(BackpressureStrategy strategy) {
            this.backpressureStrategy = strategy;
            return this;
        }

        /**
         * Sets the snapshot event filter.
         *
         * @param snapshotEventFilter Filter for snapshot events.
         * @return this
         */
        public Builder<T> snapshotEventFilter(Predicate<T> snapshotEventFilter) {
            this.snapshotEventFilter = snapshotEventFilter;
            return this;
        }

        /**
         * Sets the update event filter.
         *
         * @param updateEventFilter Filter for update events.
         * @return this
         */
        public Builder<T> updateEventFilter(Predicate<T> updateEventFilter) {
            this.updateEventFilter = updateEventFilter;
            return this;
        }

        /**
         * Builds the SnapshotPrepender.
         *
         * @return SnapshotPrepender instance.
         */
        public SnapshotPrepender<T> build() {
            Objects.requireNonNull(snapshot, "Snapshot stream must not be null");
            Objects.requireNonNull(updates, "Updates stream must not be null");
            return new SnapshotPrepender<>(snapshot, updates, backpressureStrategy, skipIfSeenInSnapshot, snapshotEventFilter, updateEventFilter);
        }
    }

    /**
     * @param <T> Type of the items in the snapshot and updates streams.
     * @return A builder for creating a SnapshotPrepender.
     */
    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    /**
     * Private constructor to prevent instantiation without using the builder.
     *
     * @param snapshot             Snapshot stream. This is a cold stream that will be streamed before the updates stream.
     * @param updates              Updates stream. This is a hot stream that will be streamed after the snapshot stream.
     * @param backpressureStrategy Backpressure strategy. Default is BUFFER.
     * @param skipIfSeenInSnapshot If true, skip update stream items that were seen in the snapshot phase. Default is false.
     * @param snapshotEventFilter  Filter for snapshot events. Default is no filter.
     * @param updateEventFilter    Filter for update events. Default is no filter.
     */
    protected SnapshotPrepender(@NonNull Flux<T> snapshot,
                                @NonNull Flux<T> updates,
                                BackpressureStrategy backpressureStrategy,
                                boolean skipIfSeenInSnapshot,
                                Predicate<T> snapshotEventFilter,
                                Predicate<T> updateEventFilter) {
        this.snapshot = snapshot;
        this.updates = updates;
        this.backpressureStrategy = backpressureStrategy;
        this.skipIfSeenInSnapshot = skipIfSeenInSnapshot;
        this.snapshotEventFilter = snapshotEventFilter;
        this.updateEventFilter = updateEventFilter;
    }

    /**
     * This method is called after the snapshot phase is built, but before the updates are added.
     * It allows subclasses to modify the snapshot phase before it is concatenated with the updates.
     *
     * @param snapshotPhase The snapshot phase built from the snapshot stream.
     * @return The modified snapshot phase.
     */
    @NonNull
    protected Flux<T> afterSnapshotPhaseBuilt(@NonNull Flux<T> snapshotPhase) {
        return snapshotPhase; // subclasses can override and modify
    }

    /**
     * This method is called after the buffered updates phase is built, but before the live updates are added.
     * It allows subclasses to modify the buffered updates phase before it is concatenated with the live updates.
     *
     * @param bufferedUpdatesPhase The buffered updates phase built from the updates stream.
     * @return The modified buffered updates phase.
     */
    @NonNull
    protected Flux<T> afterBufferedUpdatesPhaseBuilt(@NonNull Flux<T> bufferedUpdatesPhase) {
        return bufferedUpdatesPhase; // subclasses can override and modify
    }

    /**
     * This method is called after the live updates phase is built. It allows subclasses to modify the live updates phase before it is concatenated with the live updates.
     *
     * @param liveUpdatesPhase The buffered updates phase built from the updates stream.
     * @return The modified buffered updates phase.
     */
    @NonNull
    protected Flux<T> afterLiveUpdatesPhaseBuilt(@NonNull Flux<T> liveUpdatesPhase) {
        return liveUpdatesPhase; // subclasses can override and modify
    }

    /**
     * Sets the snapshot filter. This is used to filter items in the snapshot phase.
     *
     * @param filter The filter to use for the snapshot phase.
     */
    protected void setSnapshotFilter(Predicate<T> filter) {
        this.snapshotEventFilter = filter != null ? filter : t -> true;
    }

    /**
     * Sets the update filter. This is used to filter items in the updates phase.
     *
     * @param filter The filter to use for the updates phase.
     */
    protected void setUpdateFilter(Predicate<T> filter) {
        this.updateEventFilter = filter != null ? filter : t -> true;
    }

    public Flux<T> asFlux() {
        Set<T> seen = skipIfSeenInSnapshot ? ConcurrentHashMap.newKeySet() : Collections.emptySet();
        Flux<T> cachedSnapshot = snapshot.cache();
        Mono<Void> snapshotDone = cachedSnapshot.then().cache();

        ConnectableFlux<T> hotUpdates = updates.replay();
        Disposable connection = hotUpdates.connect();

        Flux<T> snapshotPhase = cachedSnapshot
                .filter(snapshotEventFilter)
                .doOnNext(t -> {
                    if (skipIfSeenInSnapshot) {
                        seen.add(t);
                    }
                })
                .doOnComplete(() ->
                        log.info("Snapshot completed")
                )
                .doOnError(e -> log.error("Snapshot error", e));

        Flux<T> bufferedUpdates = hotUpdates
                .takeUntilOther(snapshotDone)
                .filter(t -> !skipIfSeenInSnapshot || !seen.contains(t))
                .filter(updateEventFilter)
                .doOnComplete(() -> {
                    log.info("Buffered updates completed");
                    if (skipIfSeenInSnapshot) {
                        seen.clear();
                    }
                })
                .doOnError(e -> log.error("Buffered updates error", e));

        Flux<T> liveUpdates = hotUpdates
                .skipUntilOther(snapshotDone)
                .filter(updateEventFilter)
                .doOnComplete(() ->
                        log.info("Live updates completed")
                )
                .doOnError(e -> log.error("Live updates error", e));

        Flux<T> merged = Flux.concat(
                afterSnapshotPhaseBuilt(snapshotPhase),
                afterBufferedUpdatesPhaseBuilt(bufferedUpdates),
                afterLiveUpdatesPhaseBuilt(liveUpdates)
        ).doFinally(signal -> {
            connection.dispose();
            log.info("SnapshotPrepender finished with signal: {}", signal);
        });

        return switch (backpressureStrategy) {
            case DROP -> merged.onBackpressureDrop();
            case BUFFER -> merged.onBackpressureBuffer();
            default -> merged.onBackpressureError();
        };
    }


    private static final Logger log = LoggerFactory.getLogger(SnapshotPrepender.class);

    private final Flux<T> snapshot;
    private final Flux<T> updates;
    private final BackpressureStrategy backpressureStrategy;
    private final boolean skipIfSeenInSnapshot;
    private volatile Predicate<T> snapshotEventFilter;
    private volatile Predicate<T> updateEventFilter;
}


