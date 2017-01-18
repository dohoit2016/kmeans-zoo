package vn.admicro.bigdata.zookeeper;

import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.KeeperState;

public class Main {

	public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
		// TODO Auto-generated method stub
		
		ZooKeeperConnection connection = new ZooKeeperConnection();
		ZooKeeper zoo = connection.connect("localhost");
		
		zoo.create("/e_test", "DataTest".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
//		zoo.create("/e_test/test1", "DataTest".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
		
//		System.out.println(new String(zoo.getData("/kmeans", true, null), "UTF-8"));
		byte[] raw = zoo.getData("/kmeans", true, null);
		System.out.println("raw: " + raw);
		String originalString = new String(raw, "UTF-8");
		System.out.println("origin: " + originalString);
		String lines[] = originalString.split("\n");
		for(int i = 0; i < lines.length; i++){
			System.out.println(i + ": " + lines[i]);
		}
		
		connection.close();
		
	}

}
