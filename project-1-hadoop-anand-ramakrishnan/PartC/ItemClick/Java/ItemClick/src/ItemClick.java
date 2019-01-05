import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class ItemClick {
	public static HashMap<Integer, String> map = new HashMap<Integer, String>();
	public static class TokenizeMapper extends Mapper<Object, Text, Text, IntWritable>{
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			StringTokenizer st = new StringTokenizer(value.toString());
			IntWritable one = new IntWritable(1);
			while(st.hasMoreTokens()) {
				
				String s = st.nextToken();
				String[] arrOfStr = s.split("-", 2); 
				
			    for (int i =0 ; i<arrOfStr.length; i++) {
			    	if (i%2!=0) {
			    		if (arrOfStr[i].substring(0, 2).equals("04")) {
		    			
		    			String _itemID = arrOfStr[i].substring(20,29);
		    			context.write(new Text (_itemID), one);;
		    			}
			    	}
			    }
			}
		}
	}
	
	public static class ReduceClass extends Reducer<Text, IntWritable , Text, Text> {

		     @Override
		     public void reduce (Text key, Iterable<IntWritable> values, Context context)
		                 throws IOException, InterruptedException {
		    	 int count = 0;
		         for (IntWritable value : values) {
		    
						count += value.get();
		         }
		         map.put(count , key.toString()); 
		     } 
		     public void cleanup(Context context) throws IOException, InterruptedException {
		    	 int N = 0; 
		    	 Map<Integer, String>  sorted_map = new TreeMap<Integer, String>(Collections.reverseOrder());
			 		sorted_map.putAll(map);
			 		for(Integer key : sorted_map.keySet()) {	
			 			N = N + 1;
			 			if (N>10) {
			 				break;
			 			}
			 			context.write(new Text(sorted_map.get(key)), new Text(Integer.toString(key)));
			 		}
		 	}

	  } 
	  
	  public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
		
		if(otherArgs.length!=2) {
			System.exit(2);
		}
		
		Job job = Job.getInstance(conf, "Frequency");
		job.setJarByClass(ItemClick.class);
		job.setMapperClass(TokenizeMapper.class);
		job.setReducerClass(ReduceClass.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		boolean status= job.waitForCompletion(true);
		if (status) {
			System.exit(0);
		}
		else {
			System.exit(1);
		}
		

	}
}

		