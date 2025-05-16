package com.pragmafs.demo;

import reactor.core.publisher.Flux;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;

// TODO - we could make workingSet a map from id to seqNum and skip omitting
//   updates that are older tha the snapshot value
public class LimitedSizeSnapshotPrepender extends SnapshotPrepender<Item> {

    private final int maxSize;
    private final Set<String> workingSet;

    public LimitedSizeSnapshotPrepender(Flux<Item> snapshot, Flux<Item> updates, int maxSize) {
        super(snapshot,
                updates,
                BackpressureStrategy.BUFFER,
                false,
                t -> true,
                t -> true,
                t -> true
        );

        super.setSnapshotFilter(new SnapshotFilterStrategy());
        super.setBufferedUpdateFilter(new UpdateFilterStrategy());
        super.setUpdateFilter(new UpdateFilterStrategy());

        this.maxSize = maxSize;
        this.workingSet = ConcurrentHashMap.newKeySet();
    }

    private class SnapshotFilterStrategy implements Predicate<Item> {
        public boolean test(Item item) {
            if (workingSet.size() < maxSize) {
                workingSet.add(item.id);
                return true;
            }
            return false;
        }
    }

    private class UpdateFilterStrategy implements Predicate<Item> {
        public boolean test(Item item) {
            if (item.value == -1) {
                return workingSet.remove(item.id);
            }

            if (workingSet.contains(item.id)) {
                return true;
            }

            if (workingSet.size() < maxSize) {
                workingSet.add(item.id);
                return true;
            }
            return false;
        }
    }
}


