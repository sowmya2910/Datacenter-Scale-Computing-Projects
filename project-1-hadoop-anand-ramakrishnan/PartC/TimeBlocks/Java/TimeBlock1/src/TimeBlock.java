
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TimeBlock extends Configured implements Tool {

  public static HashMap<Integer, String> map = new HashMap<Integer, String>();
  public static class MapClass
                      extends Mapper<LongWritable, Text, Text, IntWritable> {

     public void map(LongWritable key, Text value, Context context)
                     throws IOException, InterruptedException {

    			String[] split_by_delimit = value.toString().split(",");
    			int revenue = Integer.parseInt(split_by_delimit[3]) * Integer.parseInt(split_by_delimit[4]);
    			int start = split_by_delimit[1].indexOf('T');
    			int end = split_by_delimit[1].indexOf('Z');
    			String _time = split_by_delimit[1].substring(start+1, end);
    			String[] _hour = _time.split(":");		
    			context.write(new Text(_hour[0]),new IntWritable(revenue));
    		  

     } 
  } 


  public static class ReduceClass extends Reducer<Text, IntWritable , Text, Text> {


     @Override
     public void reduce (Text key, Iterable<IntWritable> values, Context context)
                 throws IOException, InterruptedException {

    	 	int totalRevenue = 0;
            for (IntWritable value : values) {
            	totalRevenue += value.get();
            }
            map.put(totalRevenue , key.toString() ); 
     } 
     public void cleanup(Context context) throws IOException, InterruptedException {
 		Map<Integer, String>  sorted_map = new TreeMap<Integer, String>(Collections.reverseOrder());
 		sorted_map.putAll(map);
 		for(Integer key : sorted_map.keySet()) {			
 			context.write(new Text(sorted_map.get(key)), new Text(Integer.toString(key)));
 		}
 	}

  } 
  

  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    Job job = new Job(conf, "TopK");
    job.setJarByClass(TimeBlock.class);

    Path in = new Path(args[0]);
    Path out = new Path(args[1]);
    FileInputFormat.setInputPaths(job, in);
    FileOutputFormat.setOutputPath(job, out);

    job.setMapperClass(MapClass.class);
    job.setReducerClass(ReduceClass.class);
    job.setNumReduceTasks(1);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);

    System.exit(job.waitForCompletion(true)?0:1);

    return 0;
    
  } 

  public static void main(String[] args) throws Exception {
     int res = ToolRunner.run(new Configuration(), new TimeBlock(), args);

     System.exit(res);
  } 

} 
