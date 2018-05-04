package hw1q4;



import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class jobch {
	// step 1
	public static class FriendMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		HashMap<String, String> map = null;

		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] kValue = line.split("\\t");
			if (kValue.length < 2) {
				return;
			}

			String k = kValue[0];

			String[] values = kValue[1].split(",");
			String output = "";
			for (int i = 0; i < values.length; i++)

			{
				output += map.get(values[i]) + ",";

			}

			context.write(new Text(k), new Text(output)); //write(userId, each corresponding friendId's age))
           // get the user Id as a key,all his friend as a value
		}

		public void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);

			map = new HashMap<>();
			Configuration conf = context.getConfiguration();
			String myfilepath = conf.get("path");
			// e.g /user/hue/input/
			Path part = new Path("hdfs://cshadoop1" + myfilepath);// Location of
																	// file in
																	// HDFS

			FileSystem fs = FileSystem.get(conf);
			FileStatus[] fss = fs.listStatus(part);
			for (FileStatus status : fss) {
				Path pt = status.getPath();

				BufferedReader br = new BufferedReader(new InputStreamReader(
						fs.open(pt)));
				String line;
				line = br.readLine();
				while (line != null) {

					String[] strs = line.split(",");
					String[] value = strs[9].split("/");
					String valuess = String.valueOf((2016 - Integer     //date of birth:02/17/1988
							.valueOf(value[2])));
					map.put(strs[0], valuess);   key-pair(userId: age)
					line = br.readLine();
					//use a path to read the datauser file to get the age
				}

			}

		}

	}

	public static class FriendReducer extends Reducer<Text, Text, Text, Text> {

		protected void reduce(Text output, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {

			Iterator<Text> value1 = values.iterator(); //build a iterator value1
			int sum=0, avg=0;
			while (value1.hasNext()) {
				String[] values2 = value1.next().toString().split(",");

				for (String t : values2) {

					sum += Integer.valueOf(t);

				}

				avg = sum / values2.length;
			}

			context.write(output, new Text(String.valueOf(avg)));
			
			//get the userId as a key,get the  his average age as value.(userId->friends Id->friend's average age)
		}
	}

	// step 2

	public static class FriendMapper22 extends
			Mapper<LongWritable, Text, Text, Text> {

		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] kkValue = line.split(",");

			String k = kkValue[0];
			String address = kkValue[3] + "," + kkValue[4] + "," + kkValue[5];

			context.write(new Text(k), new Text(address));
           //read the datauser file, get the userId as key, address,state, zipcode as a value
		}
	}

	public static class FriendMapper21 extends
			Mapper<LongWritable, Text, Text, Text> {

		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] kkValue = line.split("\\t");

			String k = kkValue[0];
			String age = kkValue[1];

			context.write(new Text(k), new Text(age));
           //get the soc-liv file, use the userId as a key , his friend's average age as value
		}
	}

	public static class FriendReducer2 extends Reducer<Text, Text, Text, Text> {

		protected void reduce(Text output, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {

			Iterator<Text> value1 = values.iterator();
			String[] strs = new String[2];
			while (value1.hasNext()) {
				String s = value1.next().toString();
				if (s.contains(",")) {
					strs[1] = s;
				} else {
					strs[0] = s;
				}
			}
			if(strs[0]==null||"".equals(strs[0])){
				return;
			}
			context.write(output, new Text(strs[0] + ";" + strs[1]));
			//use userId as a key, age is a value, age(left) and address(right) as value

		}
	}

	public static class FriendMapper3 extends

	Mapper<LongWritable, Text, IntWritable, Text> {
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] k3Value = line.split("\\t");
			
			String[] k3 = k3Value[1].split(";");
            //split the address and 
			context.write(new IntWritable(Integer.parseInt(k3[0])*-1), new Text(
					k3Value[0] + ";" + k3[1]));

		}

	}

	public static class FriendReducer3 extends
			Reducer<IntWritable, Text, Text, Text> {
		private int num=0;
              //input key value output key value
		protected void reduce(IntWritable output, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
//			Iterator<Text> it = values.iterator();
			
			String age = output.get()*-1+"";
			for (Text it: values) {
				if(num>=20){
					return;
				}
				String value = it.toString();				
				String[] valuess = value.split(";");
				num++;
				context.write(new Text(valuess[0]), new Text(valuess[1] + ","
						+ age));

			}

		}

	}

	public static void main(String args[]) throws Exception {
		// Standard Job setup procedure.
		Runtime.getRuntime().exec("hdfs dfs -mkdir /yxt142830/temp");
		Configuration conf = new Configuration();
		conf.set("path", args[1]);
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = Job.getInstance(conf, "1");
		job.setJarByClass(jobch.class);
		job.setMapperClass(FriendMapper.class);
		job.setReducerClass(FriendReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path("/yxt142830/temp/1"));

		if (job.waitForCompletion(true) == true) {

			Job job2= Job.getInstance(conf, "2");

			job2.setJarByClass(jobch.class);
			job2.setReducerClass(FriendReducer2.class);
			MultipleInputs.addInputPath(job2, new Path("/yxt142830/temp/1"),
					TextInputFormat.class,FriendMapper21.class);
			MultipleInputs.addInputPath(job2, new Path(otherArgs[1]),
					TextInputFormat.class,FriendMapper22.class);

			// set output key type

			job2.setOutputKeyClass(Text.class);
			// set output value type
			job2.setOutputValueClass(Text.class);

			// set the HDFS path of the input data
			// set the HDFS path for the output
			FileOutputFormat.setOutputPath(job2, new Path("/yxt142830/temp/2"));
			if (job2.waitForCompletion(true) == true) {

				Job job3= Job.getInstance(conf, "3");

				job3.setJarByClass(jobch.class);
				job3.setReducerClass(FriendReducer3.class);
				job3.setMapperClass(FriendMapper3.class);
				// set output key type

				job3.setOutputKeyClass(IntWritable.class);
				// set output value type
				job3.setOutputValueClass(Text.class);

				// set the HDFS path of the input data
				// set the HDFS path for the output
				FileInputFormat.addInputPath(job3, new Path("/yxt142830/temp/2"));
				FileOutputFormat.setOutputPath(job3, new Path(otherArgs[2]));
				if(job3.waitForCompletion(true)){
					Runtime.getRuntime().exec("hdfs dfs -rm -r /yxt142830/temp");
					System.exit(0);
				}else{
					System.exit(1);
				}

			}

		}

	}

}
