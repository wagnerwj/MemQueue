
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;


public class MemoryQueue<T> {

	
	private BlockingQueue<T> queue;
	private final List<Consumer<T>> listeners=new ArrayList<Consumer<T>>();
	private int maxSize=500;
	
	
	private ThreadPoolExecutor consumerTaskExecutor;
	private ExecutorService dequeueTaskExecutor;
	private RejectedExecutionHandler handler;
	
	private ExecutorService sendExecutor=Executors.newSingleThreadExecutor();
	
	
	
	private final AtomicBoolean running=new AtomicBoolean(false);
	private final AtomicBoolean restarting=new AtomicBoolean(false);
	private final Class<T> type;
	
	
	private int minConcurrentConsumers=1;
	private int maxConcurrentcomsumers=1;
	
	
	public MemoryQueue(){
	 this.type=null;	
	}
	public MemoryQueue(Class<T> type){
		this.type=type;
	}
	
	public void start(){
		if(running.get()){
			return;
		}
		
		if(queue == null || this.restarting.get()){
			if(maxSize<1){
				queue=new LinkedBlockingQueue<T>();
			} else{
				queue=new LinkedBlockingQueue<T>(maxSize);
			}
		}
		if(listeners==null){
			return;
		}
		
		if(consumerTaskExecutor==null || consumerTaskExecutor.isShutdown()){
			if(handler==null){
				handler=new CallerBlocksPolicy(this.getClass());
			}
			consumerTaskExecutor=new ThreadPoolExecutor(minConcurrentConsumers,maxConcurrentcomsumers, 30L,
					TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(maxSize),handler);
		}
		
		if(dequeueTaskExecutor==null || dequeueTaskExecutor.isShutdown()){
			dequeueTaskExecutor=Executors.newSingleThreadExecutor();
		}
		dequeueTaskExecutor.submit(new Runnable(){
			public void run(){
				while(running.get()){
					final T item=receive();
					consumerTaskExecutor.submit(new Callable<T>(){
						public T call() throws Exception{
							try{
								for(Consumer<T> consumer:listeners){
									consumer.onReceive(item);
								}
							} catch (Throwable e){
								e.printStackTrace();
							}
							return item;
						}
					});
				}
			}
		});
		running.set(true);
	}
	
	
	
	public void stop(){
		if(dequeueTaskExecutor != null && !dequeueTaskExecutor.isShutdown()){
			dequeueTaskExecutor.shutdown();
		}
		if(consumerTaskExecutor!= null && ! consumerTaskExecutor.isShutdown()){
			consumerTaskExecutor.shutdown();
		}
		
		running.set(false);
	}
	public T receive(){
		T item=null;
		try{
			item=queue.take();
		}catch(InterruptedException e){
			e.printStackTrace();
		}
		return item;
	}
	
	
	public void send(T event){
		try {
			queue.put(event);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			
		}
	}
	public int getMaxSize() {
		return maxSize;
	}
	public void setMaxSize(int maxSize) {
		this.maxSize = maxSize;
	}
	public int getMinConcurrentConsumers() {
		return minConcurrentConsumers;
	}
	public void setMinConcurrentConsumers(int minConcurrentConsumers) {
		this.minConcurrentConsumers = minConcurrentConsumers;
	}
	public int getMaxConcurrentcomsumers() {
		return maxConcurrentcomsumers;
	}
	public void setMaxConcurrentcomsumers(int maxConcurrentcomsumers) {
		this.maxConcurrentcomsumers = maxConcurrentcomsumers;
	}
	public void addListener(Consumer<T> c){
		listeners.add(c);
	}
}
