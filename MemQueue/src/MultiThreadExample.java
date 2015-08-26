
import java.util.concurrent.CountDownLatch;


public class MultiThreadExample {
	
	public static void main(String[] args){
		int maxConsumers=1;
		int messageCount=10000;
		int maxQueueSize=50;
		

		Consumer<String> consumer=new Consumer<String>();
		CountDownLatch latch=new CountDownLatch(messageCount);
		consumer.setLatch(latch);
		
		MemoryQueue<String> queue=new MemoryQueue<String>();
		queue.setMaxConcurrentcomsumers(maxConsumers);
		queue.setMaxSize(maxQueueSize);
		queue.addListener(consumer);
		queue.start();
		long start=System.currentTimeMillis();
		  for (int i = 1; i <= messageCount; i++) {
	            queue.send("MSG: " + i);
	        }
	       try {
			consumer.getLatch().await();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	       long duration=System.currentTimeMillis()-start;
	       
	       queue.stop();
	       System.out.println("Done with test! Time: "+duration+" millis, average time "+((double)duration/(double)messageCount));
	       System.exit(0);
	}

}
