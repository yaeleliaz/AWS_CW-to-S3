package com.test;

public class BatchMetaInf {
    long start; // Timestamp of the oldest event in the batch
    long end; // Timestamp of the newest event in the batch
    int count; // How many events are in this batch
    int size; // The total size of all events

    public long getStart() {
        return start;
    }

    public void setStart(long start) {
        this.start = start;
    }

    public long getEnd() {
        return end;
    }

    public void setEnd(long end) {
        this.end = end;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }
}
