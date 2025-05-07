package com.pragmafs.demo;

import reactor.core.publisher.Flux;

import java.util.HashSet;
import java.util.Set;

// TODO - we could make workingSet a map from id to seqNum and skip omitting
//   updates that are older tha the snapshot value
public class LimitedSizeSnapshotPrepender extends SnapshotPrepender<Item> {

    private final int maxSize;
    private final Set<String> workingSet;

    public LimitedSizeSnapshotPrepender(Flux<Item> snapshot, Flux<Item> updates, int maxSize) {
        super(snapshot, updates);
        this.maxSize = maxSize;
        this.workingSet = new HashSet<>();
    }

    @Override
    protected void emitSnapshotMaybe(Item item) {
        if (workingSet.size() < maxSize) {
            workingSet.add(item.id);
            tryEmitNext(item);
        }
    }

    @Override
    protected void emitUpdateMaybe(Item item) {
        if (item.value == -1) {
            // indicates deleting the row. If it was part of our working set,
            // remove it and emit the update to let clients know they should
            // remove it. Otherwise no action needed.
            if (workingSet.remove(item.id)) {
                tryEmitNext(item);
            }
        } else if (workingSet.contains(item.id)) {
            tryEmitNext(item);
        } else if (workingSet.size() < maxSize) {
            workingSet.add(item.id);
            tryEmitNext(item);
        }
    }
}
