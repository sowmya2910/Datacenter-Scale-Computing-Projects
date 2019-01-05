

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SuccessRate extends Configured implements Tool {

  public static HashMap<String, Double> map = new HashMap<String, Double>();
  public static int N=0;
  public static class MapClass
                      extends Mapper<LongWritable, Text, Text, Text> {
	 @Override
     public void map(LongWritable key, Text value, Context context)
                     throws IOException, InterruptedException {
 			Text keyEmit = new Text();
    	 	String line=value.toString();
			String[] words=line.split(",");
			keyEmit.set(words[2]);
			if(words.length == 5 )
				context.write(keyEmit, new Text("2"));
			else
				context.write(keyEmit, new Text("1"));
     } 
  } 


  public static class ReduceClass extends Reducer<Text, Text , Text, Text> {

	  Text valEmit = new Text();
		

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException , InterruptedException
		{
			int count_buy = 0;
			int count_click = 0;
			for(Text value:values)
			{
				int val = Integer.parseInt(value.toString());
				if(val == 2) {
					count_buy+=1;
				}
				if(val == 1) {
					count_click+=1;
				}
			}
			if(count_click !=0) {
				Double successrate = (double) (count_buy/count_click);
				map.put( key.toString() , successrate); 
			} else {
				map.put(key.toString() , 1.0 ); 
			}
		}
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			ValueComparator bvc = new ValueComparator(map);
	        TreeMap<String, Double> sorted_map = new TreeMap<String, Double>(bvc);
			sorted_map.putAll(map);
			for(String key1 : sorted_map.keySet()) {
				N=N+1;
				if(N>10)
					break;
				context.write(new Text(key1),new Text(" "));
			}
		}

  } 
  public static class ValueComparator implements Comparator<String> {
	    Map<String, Double> base;

	    public ValueComparator(Map<String, Double> base) {
	        this.base = base;
	    }

	    // Note: this comparator imposes orderings that are inconsistent with
	    // equals.
	    public int compare(String a, String b) {
	        if (base.get(a) <= base.get(b)) {
	            return -1;
	        } else {
	            return 1;
	        } // returning 0 would merge keys
	    }
	}
  

  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    Job job = new Job(conf, "TopK");
    job.setJarByClass(SuccessRate.class);

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
    job.setMapOutputValueClass(Text.class);

    System.exit(job.waitForCompletion(true)?0:1);

    return 0;
    
  } 

  public static void main(String[] args) throws Exception {
     int res = ToolRunner.run(new Configuration(), new SuccessRate(), args);

     System.exit(res);
  } 

} 
