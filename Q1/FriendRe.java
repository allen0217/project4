import java.io.IOException;
import java.util.*;
import java.lang.Math;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



public class FriendRe {
	public static class FriendMapper extends Mapper<LongWritable,Text,Text,Text> {

	    @Override
	    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        String line=value.toString();
	        String[] kValue=line.split("\\t");
	        if(kValue.length<2)
	        {
	            return;
	        }
	        
	        String k=kValue[0];

	        String[] values=kValue[1].split(",");
	        for(int i=0;i<values.length;i++)
	        	//use two for loop to make sure that all k is to pair a friend in the friendlist
	        {
	            context.write(new Text(k),new Text("$"+values[i]));
	            for(int j=0;j<values.length;j++)
	            {
	                if(i==j)
					{
	                    continue;
	                }
	                context.write(new Text(values[i]),new Text(values[j]));
	             

	            }
	        }


	    }
	}
	
	public static class FriendReducer extends Reducer<Text,Text,Text,Text> {

	    
	    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	        
	    	HashSet<String> dele=new HashSet<String>();
	    	//build the hashset to make sure the repete element
	    	HashMap<String,Integer> sum=new HashMap<String,Integer>();
	        

	        for(Text iid:values)
	        {
	            String id=iid.toString();
	            if(id.charAt(0)=='$')
	            {
	            
	            	
	                dele.add(id.substring(1));
	            }
	            else
	            {
	                if(sum.containsKey(id))
	                {
	                    sum.put(id,sum.get(id)+1);
	                }
	                else
	                {
	                    sum.put(id,1);
	                }
	            }
	        }
	        for(String id:dele)
	        	//remove the duplicate element to make sure no first friend
	        {
	            sum.remove(id);
	        }
	        if(sum.size()==0)
	        {
	            return;
	        }
	        List<Map.Entry<String,Integer>> friendlist=new ArrayList<Map.Entry<String,Integer>>();
	        // use the list to judge the value to sort the key order
	        friendlist.addAll(sum.entrySet());
	        
	        ValueComparator valueC=new ValueComparator();
	        
	        Collections.sort(friendlist,valueC);
	        
	        String op="";
	        
	        for(int i=0;i<friendlist.size()&&i<10;i++)
	        {
	            Map.Entry<String,Integer> entry=friendlist.get(i);
	            op= op+entry.getKey()+",";
	        }
	        op=op.substring(0,op.length()-1);
	        context.write(key,new Text(op));

	    }
	}

	static class ValueComparator implements Comparator<Map.Entry<String, Integer>>
	//compartor method to order
	{
	    public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2)
	    {
	        return (o2.getValue()).compareTo(o1.getValue());
	    }
	}
	
	public static void main(String args[]) throws Exception {
		
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "Mutual Friends");
		job.setJarByClass(FriendRe.class);
		job.setMapperClass(FriendMapper.class);
		job.setReducerClass(FriendReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
}
