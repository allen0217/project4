import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.util.*;

public class MutualFriends {

	public static class FriendsMapper extends
			Mapper<LongWritable, Text, Text, Text> {
	

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String twoFriend = context.getConfiguration().get("ARGUMENT");
			String[] twoFriends = twoFriend.split(",");

			String line = value.toString();
			String[] kv = line.split("\\t");

			String k = kv[0];
			String valuess = kv.length > 1 ? kv[1] : "";

			if (k.equals(twoFriends[0]) || k.equals(twoFriends[1]))
				context.write(new Text(twoFriend), new Text(valuess));
		}
	}

	public static class FriendsReducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> valuess, Context context)
				throws IOException, InterruptedException {

			Iterator<Text> values1 = valuess.iterator();
			StringBuilder sb = new StringBuilder();
			HashMap<String, Integer> count = new HashMap<String, Integer>();

			while (values1.hasNext()) {

				String[] values2 = values1.next().toString().split(",");
				for (String t : values2) {
					String id = t.toString();

					if (count.containsKey(id)) {
						count.put(id, count.get(id) + 1);
					} else {
						count.put(id, 1);
					}

					if (count.get(id) >= 2) {
						sb = sb.append(id);
						sb = sb.append(",");
					}
				}

			}
			context.write(key, new Text(sb.toString()));
		}

	}

	public static void main(String args[]) throws Exception {
		// Standard Job setup procedure.
		Configuration conf = new Configuration();

		String twoFriend = args[2];
		conf.set("ARGUMENT", twoFriend);

		Job job = Job.getInstance(conf, "Mutual Friends");
		job.setJarByClass(MutualFriends.class);
		job.setMapperClass(FriendsMapper.class);
		job.setReducerClass(FriendsReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}