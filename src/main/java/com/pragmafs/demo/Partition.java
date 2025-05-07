package com.pragmafs.demo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

class Partition implements Iterable<Item> {

    private static final Logger log = LoggerFactory.getLogger(Partition.class);
    volatile int lastIdx = -1; // the last valid index
    volatile Item[] data;
    final static int INITIAL_SIZE = 5;
    final static int MAX_SIZE = 256_000_000;

    Partition() {
        data = new Item[INITIAL_SIZE];
    }

    int size() {
        return lastIdx + 1;
    }

    Item get(int idx) {
        if (idx > lastIdx)
            throw new IndexOutOfBoundsException();
        return data[idx];
    }

    void add(Item item) {
        ensureCapacity();
        data[lastIdx + 1] = item;
        lastIdx++; // has to come after data is added for concurrent read to be safe. ++ok because we're the only writer.
        // log.info("Added to cache: {}", item);
    }

    void ensureCapacity() {
        if (lastIdx == data.length - 1) {
            int newSize = data.length << 1;
            if (newSize > MAX_SIZE) {
                throw new IllegalStateException(String.format("Request to add beyond maximum size. Current length is %d max is %d", data.length, MAX_SIZE));
            }
            data = Arrays.copyOf(data, newSize); // ok because we are the single writer
        }
    }

    @Override
    public Iterator<Item> iterator() {
        return new Iterator<>() {

            final int lastIdxSnap = Partition.this.lastIdx;
            final Item[] dataSnap = Partition.this.data;
            int next = 0;

            @Override
            public boolean hasNext() {
                return next <= lastIdxSnap;
            }

            @Override
            public Item next() {
                if (!hasNext())
                    throw new NoSuchElementException();
                return dataSnap[next++];
            }
        };
    }

}
