
import java.io.IOException;

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

public class TopKValues extends Configured implements Tool {

  private static int N = 0 ;

  public static class MapClass
                      extends Mapper<LongWritable, Text, IntWritable, Text> {

     public void map(LongWritable key, Text value, Context context)
                     throws IOException, InterruptedException {

            String[] string_word = value.toString().split("\\s+") ;
            int length = string_word.length;
            int value_cnt =  Integer.parseInt(string_word[length-1]);
            context.write(new IntWritable(value_cnt*-1), new Text(value));

            

     } 
  } 


  public static class ReduceClass extends Reducer<IntWritable, Text, Text, Text> {


     @Override
     public void reduce (IntWritable key, Iterable<Text> values, Context context)
                 throws IOException, InterruptedException {

    	 
            for (Text value : values) {
            	N=N+1;
            	if(N>2000) {
            		break;
            	}
            	String[] string_word = value.toString().split("\\s+") ;
            	String finalout = "";
            	for(int i=0;i<string_word.length-1;++i) {
            		finalout = finalout + string_word[i] + " ";
            	}
            	context.write(new Text(finalout),new Text(string_word[1]));
            }


     } 

  } 
  

  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    Job job = new Job(conf, "TopK");
    job.setJarByClass(TopKValues.class);

    Path in = new Path(args[0]);
    Path out = new Path(args[1]);
    FileInputFormat.setInputPaths(job, in);
    FileOutputFormat.setOutputPath(job, out);

    job.setMapperClass(MapClass.class);
    job.setReducerClass(ReduceClass.class);
    job.setNumReduceTasks(1);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(Text.class);

    System.exit(job.waitForCompletion(true)?0:1);

    return 0;
    
  } 

  public static void main(String[] args) throws Exception {
     int res = ToolRunner.run(new Configuration(), new TopKValues(), args);

     System.exit(res);
  } 

} 
