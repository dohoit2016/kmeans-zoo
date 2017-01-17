package vn.admicro.bigdata.zookeeper;

import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

public class Main {

	public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
		// TODO Auto-generated method stub
		
		ZooKeeperConnection connection = new ZooKeeperConnection();
		ZooKeeper zoo = connection.connect("localhost");
		
		zoo.create("/e_test", "DataTest".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
//		zoo.create("/e_test/test1", "DataTest".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
		
		
		connection.close();
		
	}

}
