package vn.admicro.bigdata.zookeeper;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import java.util.ArrayList;
import java.util.Date;
import java.util.logging.Logger;



public class MapKMeans extends Mapper<LongWritable, Text, IntWritable, Text>{
	
	public static ArrayList<Point> centroids = new ArrayList();
	
	
	@Override
	protected void setup(Mapper<LongWritable, Text, IntWritable, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);
		
		System.out.println("start");
//		centroids = KMeans.StringToArrayList(context.getConfiguration().get("centroids"));
		ZooKeeperConnection connection = new ZooKeeperConnection();
		ZooKeeper zoo = connection.connect("localhost");
		String centroidsString = null;
		try {
			byte[] raw = zoo.getData("/kmeans", true, null);
			System.out.println("raw: " + raw);
			String originalString = new String(raw, "UTF-8");
			System.out.println("origin: " + originalString);
			centroidsString = originalString;
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		centroids = KMeans.StringToArrayList(centroidsString);
		
//		centroids = KMeans.centroids;
		
		System.out.println("setup in map: size centroids = " + centroids.size());
		
		for(Point p : KMeans.centroids){
			System.out.println("setup in map: " + p.x + " : " + p.y);
		}
		
		for(Point p : centroids){
			System.out.println(p.x + " : " + p.y);
		}
		System.out.println("done");
		
	}
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		
		String line = value.toString();
		int x = Integer.parseInt(line.split("\t")[0]);
		int y = Integer.parseInt(line.split("\t")[1]);
		
		
		
		double min = KMeans.maxRadius;
		int index = 0;
		for(int i = 0; i < centroids.size(); i++){
			Point centroid = (Point) centroids.get(i);
			double dis = Math.sqrt((x - centroid.x) * (x - centroid.x) + (y - centroid.y) * (y - centroid.y));
//			System.out.println("-----");
//			System.out.println(x);
//			System.out.println(y);
//			System.out.println(centroid.x);
//			System.out.println(centroid.y);
//			System.out.println(dis);
//			System.out.println(min);
//			System.out.println("-----");
//			logger.info("Hehehehehehehehehehehe");
			if (dis < min){
				index = i;
				min = dis;
			}
		}
		try {
			IntWritable mKey = new IntWritable(index);
			Point centroid = (Point) centroids.get(index);
			String s = x + KMeans.SPLITTER;
			s = s + y;
			Text mValue = new Text(s);     
			context.write(mKey, mValue);
		}
		catch (Exception e){
//			System.out.println(e.toString());
			
//			ArrayList h = new ArrayList();
//			h.add(new Point(1, 2));

//			context.write(new IntWritable(1), new Text(e.toString() + "\nhiuhiu\n" + h.size() + "\nhiuhiu\n" + "\nhehe\n" + KMeans.centroids.size() + "\nhihi\n"));
			context.write(new IntWritable(1), new Text("Exception from Map: " + e.toString()));
		}
		
	}

}
