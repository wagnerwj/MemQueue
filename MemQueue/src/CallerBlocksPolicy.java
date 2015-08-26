

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;
import java.util.concurrent.atomic.AtomicInteger;


public class CallerBlocksPolicy extends CallerRunsPolicy {
    
    // injectable
    /** the number of rejection occurrences before logging */
    private int logInterval = 100;

    //

    // internal
    private AtomicInteger blockCount = new AtomicInteger();
    private final Object source;

    //

    public CallerBlocksPolicy() {
this("UNK");
    }

    public CallerBlocksPolicy(Object source) {
this.source = source;
    }

    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {


try {
	// block until space becomes available
	System.out.println("[" + source + "] CALLER BLOCKING: occurrences="+ blockCount.get() + ", activeThreads="
	+ executor.getActiveCount() + ", maxThreads="
	+ executor.getMaximumPoolSize());
	
	executor.getQueue().put(r);
} catch (InterruptedException e) {
	// TODO Auto-generated catch block
	e.printStackTrace();
}
    }

    public int getAndResetBlockCount() {
return this.blockCount.getAndSet(0);
    }

    public void setLogInterval(int logInterval) {
this.logInterval = logInterval;
    }

    
}


