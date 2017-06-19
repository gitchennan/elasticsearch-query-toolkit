package org.es.sql.bean;

public class RangeSegment {
    private Object from;
    private Object to;
    private SegmentType segmentType;

    public RangeSegment(Object from, Object to, SegmentType segmentType) {
        this.from = from;
        this.to = to;
        this.segmentType = segmentType;
    }

    public Object getFrom() {
        return from;
    }

    public Object getTo() {
        return to;
    }

    public SegmentType getSegmentType() {
        return segmentType;
    }

    public enum SegmentType {
        Date, Numeric
    }
}
