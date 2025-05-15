package com.pragmafs.demo;

import org.springframework.lang.NonNull;
import java.util.Objects;

public class Item {
    private final String id;
    private int value;
    private Source source;

    public enum Source { SNAP, UPDATE }

    public Item(String id, int value, Source source) {
        this.id = Objects.requireNonNull(id);
        this.source = Objects.requireNonNull(source);
        this.value = value;
    }

    public String getId() {
        return id;
    }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }

    public Source getSource() {
        return source;
    }

    public void setSource(Source source) {
        this.source = source;
    }

    @Override
    @NonNull
    public String toString() {
        return String.format("Item{id='%s', value=%d, source=%s}", id, value, source);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Item item)) return false;
        return Objects.equals(id, item.id) && value == item.value ;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, value);
    }
}
