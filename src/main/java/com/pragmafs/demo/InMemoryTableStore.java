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

    void upsert(Item update) {
        if (update != null) {
            Item row = rows.get(update.id);
            if (row == null) {
                createAndIndex(update);
            } else {
                updateRow(update, row);
            }
        }
    }

    /**
     * Creates and a new row for the given update and indexes it.
     */
    private void createAndIndex(Item update) {
        Item newItem = new Item(update.id, update.value, SNAP);
        rows.put(newItem.id, newItem);
        allPartition.add(newItem);
    }

    /**
     * Apply changes in an update to an existing row.
     */
    private void updateRow(Item update, Item row) {
        row.value = update.value;
    }
}
