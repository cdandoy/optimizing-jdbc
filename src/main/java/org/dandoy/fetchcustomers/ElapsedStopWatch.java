package org.dandoy.fetchcustomers;

import java.util.concurrent.TimeUnit;

public class ElapsedStopWatch {
    private final TimeUnit _timeUnit;
    private long _lastTime;

    public ElapsedStopWatch() {
        this(TimeUnit.MILLISECONDS);
    }

    public ElapsedStopWatch(TimeUnit timeUnit) {
        _timeUnit = timeUnit;
        _lastTime = System.currentTimeMillis();
    }

    public String toString() {
        final long now = System.currentTimeMillis();
        try {
            return TimeUnit.MILLISECONDS.convert(now - _lastTime, _timeUnit) + getUnit();
        } finally {
            _lastTime = now;
        }
    }

    private String getUnit() {
        return switch (_timeUnit) {
            case NANOSECONDS -> "ns";
            case MICROSECONDS -> "μs";
            case MILLISECONDS -> "ms";
            case SECONDS -> "s";
            case MINUTES -> "m";
            case HOURS -> "h";
            case DAYS -> "d";
        };
    }
}
