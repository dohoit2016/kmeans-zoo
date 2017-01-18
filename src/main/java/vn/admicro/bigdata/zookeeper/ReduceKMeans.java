package vn.admicro.bigdata.zookeeper;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

public class ReduceKMeans extends Reducer<IntWritable, Text, IntWritable, Text>{
	
	public static ArrayList<Point> centroids = new ArrayList();
	public static ArrayList<Point> oldCentroids = new ArrayList();
	public static int length = 0;
	public static int done = 0;
	public static String centroidsString = "";
	public static final String host = "hdfs://localhost:9000";
	ZooKeeperConnection connection = new ZooKeeperConnection();
	ZooKeeper zoo;
	
	@Override
	protected void setup(Reducer<IntWritable, Text, IntWritable, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);
//		centroids = KMeans.StringToArrayList(context.getConfiguration().get("centroids"));
		
		zoo = connection.connect("localhost");
		String centroidsString = null;
		try {
			centroidsString = new String(zoo.getData("/kmeans", true, null), "UTF-8");
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		centroids = KMeans.StringToArrayList(centroidsString);
		length = context.getConfiguration().getInt("length", 0);
		done = context.getConfiguration().getInt("done", 0);
		
//		centroids = KMeans.centroids;
//		oldCentroids = KMeans.oldCentroids;
//		oldCentroids = new ArrayList();
//		for(Point p : centroids){
//			oldCentroids.add(new Point(p.x, p.y));
//		}
		
	}
	
	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		int oldInt = context.getConfiguration().getInt("keyInt", -5);
		System.out.println("Reduce: " + context.getConfiguration().getInt("keyInt", -5));
		context.getConfiguration().setInt("keyInt", oldInt + 1);
		System.out.println("Reduce: " + context.getConfiguration().getInt("keyInt", -5));
		System.out.println("---");
		try {
			double sumx = 0;
			double sumy = 0;
			int noOfPoints = 0;
			for(Text value : values){
				String s = value.toString();
//				context.write(new IntWritable(1), new Text("for: s=" + s + "."));
				int y = Integer.parseInt(s.split(KMeans.SPLITTER)[1]);
				int x = Integer.parseInt(s.split(KMeans.SPLITTER)[0]);
				sumx += x;
				sumy += y;
				noOfPoints++;
			}
//			context.write(new IntWritable(1), new Text("Done for."));
			double cenx = sumx / noOfPoints;
			double ceny = sumy / noOfPoints;
			int index = key.get();
			System.out.println("centroid");
			Point centroid = centroids.get(index);
			System.out.println(centroid.x + " : " + centroid.y);
			centroid.x = cenx;
			centroid.y = ceny;
			//context.write(new IntWritable(1), new Text(cenx + KMeans.SPLITTER + ceny));
			done++;
			context.getConfiguration().setInt("done", done);
			centroidsString += cenx + KMeans.SPLITTER + ceny + "\n";
			context.getConfiguration().set("centroidsString", centroidsString);
			if (done >= length){
				System.out.println("done >= length");
				
				// Update centroids using HDFS
//				FileSystem fs = FileSystem.get(context.getConfiguration());
//				Path path = new Path(host + "/user/donnn/kmeans/centroids.txt");
//				fs.delete(path);
//				
//				BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(path, true)));
//				//bw.write("hehe\n");
//				bw.write(centroidsString);
//				bw.flush();
//				bw.close();
//				System.out.println("wrote");
				// End
				
				// Update centroids using ZooKeeper
				zoo.setData("/kmeans", centroidsString.getBytes(), zoo.exists("/kmeans", true).getVersion());
				// End
			}
		}
		catch (Exception e){
			try {
				throw e;
			} catch (Exception e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			String s = "";
			for(Text value : values){
				s += value.toString() + "\nnextValue\n";
			}
			//context.write(new IntWritable(1), new Text("Exception from Reduce: " + e.toString() + "haha" + s + "hichic"));
		}
		
	}

}
