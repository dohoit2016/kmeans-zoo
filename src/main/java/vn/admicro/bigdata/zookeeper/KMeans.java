package vn.admicro.bigdata.zookeeper;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import com.google.gson.Gson;

public class KMeans {

	public static final ArrayList<Point> oldCentroids = new ArrayList();
	public static final ArrayList<Point> centroids = new ArrayList();
	public static final double maxRadius = 9999999;
	public static final String SPLITTER = "___";
	public static int firstLoop = 1;
	public static final double threshold = 0.0000001;
	public static int loop = 0;
	public static final String host = "hdfs://localhost:9000";
	
	public static CountDownLatch cdl;
	public static ZooKeeperConnection connection = new ZooKeeperConnection();
	
	public static ArrayList<Point> StringToArrayList(String json){
		Gson gson = new Gson();
		
		ArrayList al = gson.fromJson(json, ArrayList.class);
		ArrayList<Point> result = new ArrayList();
		
		for(int i = 0; i < al.size(); i++){
			Point p = (Point) gson.fromJson(gson.toJson(al.get(i)), Point.class);
			result.add(p);
		}
		
		return result;
		
	}
	
	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException, KeeperException{
		
		// Implicit 3 centroids
		
		Configuration conf = new Configuration();
		conf.addResource(new Path("resource/config/core-site.xml"));
		conf.addResource(new Path("resource/config/hbase-site.xml"));
		conf.addResource(new Path("resource/config/hdfs-site.xml"));
		conf.addResource(new Path("resource/config/mapred-site.xml"));
		conf.addResource(new Path("resource/config/yarn-site.xml"));
		
		// Choose 3 random centroids
		
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(host + "/user/donnn/kmeans/nhap.in");
		
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path), "utf8"));
		String line = br.readLine();
		ArrayList<String> points = new ArrayList();
		while (line != null){
			int x = Integer.parseInt(line.split("\t")[0]);
			int y = Integer.parseInt(line.split("\t")[1]);
			points.add(new String(x + SPLITTER + y));
			line = br.readLine();
		}
		
		String centroidsString = new String("");
		Random rand = new Random();
		for(int i = 0; i < 3; i++){
			int r = rand.nextInt(points.size());
			System.out.println("getting centroids at " + r);
			centroidsString += points.get(r) + "\n";
		}
		
		System.out.println("Init centroids: " + centroidsString);
		
		
		br.close();
		
		// Save centroids to HDFS
//		path = new Path(host + "/user/donnn/kmeans/centroids.txt");
//		fs.delete(path);
//		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(path)));
//		bw.write(centroidsString);
//		bw.close();
		
		// End of saving
		
		// Save centroids to ZooKeeper
		
		ZooKeeper zoo = connection.connect("localhost");
		try {
			zoo.create("/kmeans", centroidsString.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}
		catch (Exception e){
			zoo.setData("/kmeans", centroidsString.getBytes(), zoo.exists("/kmeans", true).getVersion());
			e.printStackTrace();
		}
		
		
		
		// End of saving
		
		
		while (true){
			System.out.println("Loop: " + loop);
			loop++;
			System.out.println("Main-time: " + (new Date()).getTime());
			System.out.println("Prepare setting keyInt");
			conf.setInt("keyInt", 1);
			System.out.println("Main: " + conf.get("keyInt"));
			System.out.println("---------------");
			
			// Read centroids from HDFS
//			path = new Path(host + "/user/donnn/kmeans/centroids.txt");
//			
//			br = new BufferedReader(new InputStreamReader(fs.open(path), "utf8"));
//			line = br.readLine();
//			System.out.println("line: " + line);
//			centroids.clear();
//			while (line != null){
//				String[] nums = line.split(SPLITTER);
//				if (nums.length > 1){
//					double x = Double.parseDouble(nums[0]);
//					double y = Double.parseDouble(nums[1]);
//					centroids.add(new Point(x, y));
//				}
//				line = br.readLine();
//				System.out.println("line: " + line);
//			}
//			
//			System.out.println("Main: size centroids = " + centroids.size());
//			
//			
//			br.close();
			// End
			
			// Read centroids from ZooKeeper
			byte[] raw = zoo.getData("/kmeans", true, null);
			System.out.println("raw: " + raw);
			String originalString = new String(raw, "UTF-8");
			System.out.println("origin: " + originalString);
			String lines[] = originalString.split("\n");
			centroids.clear();
			for(int i = 0; i < lines.length; i++){
				line = lines[i];
				String[] nums = line.split(SPLITTER);
				if (nums.length > 1){
					double x = Double.parseDouble(nums[0]);
					double y = Double.parseDouble(nums[1]);
					centroids.add(new Point(x, y));
				}
			}
			
			System.out.println("Main: size centroids = " + centroids.size());
			// End
			
			
			
			Gson gson = new Gson();
			
//			conf.set("centroids", gson.toJson(centroids));
			zoo.setData("/kmeans", gson.toJson(centroids).getBytes(), zoo.exists("/kmeans", true).getVersion());
			conf.setInt("length", centroids.size());
			conf.setInt("done", 0);
			
			if (firstLoop == 1){
				for(int i = 0; i < centroids.size(); i++){
					Point p = centroids.get(i);
					Point p1 = new Point(p.x, p.y);
					oldCentroids.add(p1);
				}
				firstLoop = 0;
			}
			else {
				double e = 0;
				for(int i = 0; i < centroids.size(); i++){
					Point oldP = oldCentroids.get(i);
					Point newP = centroids.get(i);
					e += Math.sqrt((newP.x - oldP.x) * (newP.x - oldP.x) + (newP.y - oldP.y) * (newP.y - oldP.y));
				}
				System.out.println("error: " + e);
				if (e < threshold){
					System.out.println("break");
					break;
				}
				else {
					oldCentroids.clear();
					for(int i = 0; i < centroids.size(); i++){
						Point p = centroids.get(i);
						Point p1 = new Point(p.x, p.y);
						oldCentroids.add(p1);
					}
				}
			}
			
			
			
			
			Job job = new Job(conf);
			job.setJarByClass(KMeans.class);
			job.setJobName("KMeans");
			FileInputFormat.addInputPath(job, new Path(host + "/user/donnn/kmeans/nhap.in"));
			FileOutputFormat.setOutputPath(job, new Path(host + "/user/donnn/kmeans/kmeans.out"));
			
			job.setMapperClass(MapKMeans.class);
			job.setReducerClass(ReduceKMeans.class);
			
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(Text.class);
			
			System.out.println("Start MapRedece");
			job.waitForCompletion(true);
			
			System.out.println("Main-time: " + (new Date()).getTime());
//			System.out.println("Prepare setting keyInt");
//			conf.setInt("keyInt", 1);
			System.out.println("Main: " + conf.getInt("keyInt", -10));
			System.out.println("---------------");
			path = new Path(host + "/user/donnn/kmeans/kmeans.out");
			fs.delete(path);
			System.out.println("deleted");
		}
		
		System.out.println("out");
		String result = new String("");
		for(int i = 0; i < centroids.size(); i++){
			Point centroid = centroids.get(i);
			result += centroid.x + SPLITTER + centroid.y + "\n";
		}
		
		result = result.replace("___", "\t");
		
		
		path = new Path(host + "/user/donnn/kmeans/kmeans.out");
		
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(path)));
		bw.write(result);
		bw.close();
		
		System.out.println(result);
		
		
	}
	
}