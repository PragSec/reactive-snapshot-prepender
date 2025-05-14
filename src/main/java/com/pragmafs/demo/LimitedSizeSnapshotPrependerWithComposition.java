package com.pragmafs.demo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;

// TODO - we could make workingSet a map from id to seqNum and skip omitting
//   updates that are older tha the snapshot value
// JH - this version uses composition instead of inheritance
public class LimitedSizeSnapshotPrependerWithComposition {
    private final SnapshotPrepender<Item> snapshotPrepender;
    private final int maxSize;
    private final Set<String> workingSet;

    /**
     * Creates a LimitedSizeSnapshotPrependerWithComposition instance.
     * @param snapshot Snapshot flux.
     * @param updates Updates flux.
     * @param maxSize Maximum size of the working set. if maxSize < 1, no limit is applied.
     */
    public LimitedSizeSnapshotPrependerWithComposition(Flux<Item> snapshot, Flux<Item> updates, int maxSize) {
        snapshotPrepender = SnapshotPrepender.<Item>builder()
                .snapshot(snapshot) // Set the snapshot Flux
                .updates(updates)   // Set the updates Flux
                .skipIfSeenInSnapshot(true)
                .backpressure(SnapshotPrepender.BackpressureStrategy.BUFFER) // Set backpressure strategy
                .snapshotEventFilter(new SnapshotFilterStrategy()) // Set snapshot filter, lambda work here just as well
                .updateEventFilter(new UpdateFilterStrategy())     // Set update filter, lambda work here just as well
                .build();

        this.maxSize = maxSize;
        this.workingSet = ConcurrentHashMap.newKeySet();
    }

    public boolean hasNoSizeLimit() {
        return maxSize < 1;
    }

    /**
     * Returns a Flux that emits the items from the snapshot and updates, in order (snapshot followed by updates).
     * @return A Flux that emits the items from the snapshot and updates.
     */
    public Flux<Item> asFlux() {
        return snapshotPrepender.asFlux();
    }

    private class SnapshotFilterStrategy implements Predicate<Item> {
        private static final Logger log = LoggerFactory.getLogger(SnapshotFilterStrategy.class);
        public boolean test(Item item) {
            if (hasNoSizeLimit() || workingSet.size() < maxSize) {
                workingSet.add(item.id());
                return true;
            }
            log.info("Snapshot filter: workingSet is full: {}, item {} not added", workingSet.size(), item);
            return false;
        }
    }

    private class UpdateFilterStrategy implements Predicate<Item> {
        private static final Logger log = LoggerFactory.getLogger(UpdateFilterStrategy.class);
        public boolean test(Item item) {
            if (item.value() == -1) {
                log.info("Update filter: item {} removed", item);
                return workingSet.remove(item.id());
            }

            if (workingSet.contains(item.id())) {
                return true;
            }

            if (hasNoSizeLimit() || workingSet.size() < maxSize) {
                workingSet.add(item.id());
                return true;
            }
            log.info("Update filter: workingSet is full: {}, item {} not added", workingSet.size(), item);
            return false;
        }
    }
}
