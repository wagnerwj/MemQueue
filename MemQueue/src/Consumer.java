
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;


public class Consumer<T> {
	private CountDownLatch latch;
    private final BlockingQueue<T> msgs = new LinkedBlockingQueue<T>();
    private final AtomicInteger msgCnt = new AtomicInteger();

    private volatile boolean throwEx = false;

    public Consumer(){
    
    }
   

    public Consumer(Integer id, int msgCnt) {
   
        this.latch = new CountDownLatch(msgCnt);
    }

    public void onReceive(T msg) {
        try {
            if (!throwEx) {
                msgCnt.incrementAndGet();
                msgs.add(msg);
                System.out.println("Recv: MSG [" + msg + "]" + this);
            } else {
                throw new RuntimeException("failure test");
            }
        } finally {
            latch.countDown();
        }
    }

    public void setLatch(CountDownLatch latch) {
        this.latch = latch;
    }

    public CountDownLatch getLatch() {
        return latch;
    }

    public List<T> getMsgs() {
        return new ArrayList<T>(msgs);
    }

    public int getMsgCount() {
        return this.msgCnt.get();
    }

    public void setThrowEx(boolean throwEx) {
        this.throwEx = throwEx;
    }

    @Override
    public String toString() {
        return " Thread[" + Thread.currentThread().getId() + "] recvCnt [" + getMsgCount() + "]";
    }
}
