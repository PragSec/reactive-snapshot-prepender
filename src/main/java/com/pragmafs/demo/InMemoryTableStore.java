package com.pragmafs.demo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.pragmafs.demo.Item.Source.SNAP;

/**
 * In-memory HashMap store of Rows. Supports optional partition index.
 * <p>
 * Impl note:
 * All rows are stored in the flat table. In addition, for a partitioned table, each partition is stored as an index
 * pointing to the same row objects, to efficiently service partition snapshot requests.
 * Note we assume the partition value never changes; if it did we'd have to deal with it appropriately in upsert.
 * <p>
 * Threading impl note:
 * - rows map is only used from upsert, never select, so it's safe
 * - partitions is ConcurrentHashMap, so safe for concurrent writer & readers
 * - each Partition is thread-safe for a single writer and multiple readers
 * - each Item is thread safe - either ConcurrentHashMapDataRow or StringBackedDataRow
 * - persisterStats: TODO - confirm it's thread-safe
 */
public class InMemoryTableStore {

    private final Map<String, Item> rows; // map from id to Item to service updates
    private final Partition allPartition; // "all" partition for non-partitioned tables to service selects
    private final Logger logger = LoggerFactory.getLogger(InMemoryTableStore.class);

    InMemoryTableStore() {
        this.rows = new ConcurrentHashMap<>();
        this.allPartition = new Partition();
    }

    void upsert(Item update) {
        if (update == null)
            return;
        rows.merge(update.id(), createAndIndex(update), (oldItem, newItem) -> oldItem.withValue(newItem.value()));
    }

    Flux<Item> select() {
        Partition part = allPartition;
        if (part.isEmpty())
            return Flux.empty();

        return Flux.create(sink -> {
            Iterator<Item> iterator = part.iterator();
            sink.onRequest(n -> {
                logger.info("select flux got request for {}", n);
                for (int i = 0; i < n && iterator.hasNext(); i++) {
                    var row = iterator.next();
                    sink.next(row);
                }
                if (!iterator.hasNext()) {
                    sink.complete();
                }
            });
        });
    }

    int size() {
        return allPartition.size();
    }

    /**
     * Creates and a new row for the given update and indexes it.
     */
    private Item createAndIndex(Item update) {
        Item row = createSnapRow(update);
        allPartition.add(row);
        return row;
    }

    /**
     * Create a new Item to represent the update.
     * If this table is write-once/storage optimized, uses the string rep instead of the default map-based.
     */
    private Item createSnapRow(Item update) {
        return new Item(update.id(), update.value(), SNAP);
    }
}
