package com.pragmafs.demo;

import reactor.core.publisher.Flux;

// JH - this version uses composition instead of inheritance
public class SnapshotPrependerWithComposition {
    private final SnapshotPrepender<Item> snapshotPrepender;

    /**
     * Creates a LimitedSizeSnapshotPrependerWithComposition instance.
     *
     * @param snapshot Snapshot flux.
     * @param updates  Updates flux.
     */
    public SnapshotPrependerWithComposition(Flux<Item> snapshot, Flux<Item> updates) {
        snapshotPrepender = SnapshotPrepender.<Item>builder()
                .snapshot(snapshot) // Set the snapshot Flux
                .updates(updates)   // Set the updates Flux
                .build();
    }

    /**
     * Returns a Flux that emits the items from the snapshot and updates, in order (snapshot followed by updates).
     *
     * @return A Flux that emits the items from the snapshot and updates.
     */
    public Flux<Item> asFlux() {
        return snapshotPrepender.asFlux();
    }
}
