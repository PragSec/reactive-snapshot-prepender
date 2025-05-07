package com.pragmafs.demo;

class Item {

    public enum Source { SNAP, UPDATE }
    final String id;
    int value;

    final Source source;
    Item(String id, int value, Source source) {
        this.id = id;
        this.value = value;
        this.source = source;
    }

    @Override
    public String toString() {
        return "{%s -> %d} %s %s".formatted(id, value, source, super.toString());
    }

    public String getId() {
        return id;
    }
}
