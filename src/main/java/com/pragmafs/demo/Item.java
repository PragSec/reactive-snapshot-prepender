package com.pragmafs.demo;

import reactor.util.annotation.NonNull;

record Item(String id, int value, Source source) {

    public enum Source { SNAP, UPDATE }

    @NonNull
    public Item withValue(int newValue) {
        return new Item(this.id, newValue, this.source);
    }

    @NonNull
    public Item withValueAndSource(int newValue, Source source) {
        return new Item(this.id, newValue, source);
    }


    @Override
     @NonNull
     public String toString() {
        return String.format("Item{id='%s', value=%d, source=%s}", id, value, source);
    }

    @Override
    public boolean equals(Object o) {
        return this == o || (o instanceof Item item && id.equals(item.id) && value == item.value);
    }

    @Override
    public int hashCode() {
        return id.hashCode() * 31 + Integer.hashCode(value);
    }
}
