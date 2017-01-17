package vn.admicro.bigdata.zookeeper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;

public class ZooKeeperConnection {
	private ZooKeeper zoo;
	
	private final CountDownLatch cdl = new CountDownLatch(1);
	
	public ZooKeeper connect(String host) throws IOException, InterruptedException{
		zoo = new ZooKeeper(host, 5000, new Watcher() {
			
			public void process(WatchedEvent we) {
				// TODO Auto-generated method stub
				if (we.getState() == KeeperState.SyncConnected){
					System.out.println("connected");
					try {
						Thread.sleep(10);
						cdl.countDown();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
						cdl.countDown();
					}
				}
				
			}
			
			
		});
		System.out.println("creating zoo");
		cdl.await();
		System.out.println("returning");
		return zoo;
		
	}
	
	public void close() throws InterruptedException{
		zoo.close();
	}
}
