package org.spoorn.tarlz4java.util.concurrent;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Thread factory with a name.
 */
public class NamedThreadFactory implements ThreadFactory {

    private final AtomicInteger threadNumber = new AtomicInteger(1);
    private final String name;
    
    public NamedThreadFactory(String name) {
        this.name = name;
    }
    
    @Override
    public Thread newThread(Runnable r) {
        return new Thread(r, this.name + "-" + threadNumber.getAndIncrement());
    }
}
