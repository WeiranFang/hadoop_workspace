import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ChessDataC {

  public static class FirstMapper
       extends Mapper<Object, Text, Text, DoubleWritable>{

    private Text plyCount = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
	  long total = context.getCounter("org.apache.hadoop.mapred.Task$Counter", "MAP_INPUT_RECORDS").getValue();
      StringTokenizer itr = new StringTokenizer(value.toString());
	  while (itr.hasMoreTokens()) {
		  String label = itr.nextToken();
		  if (label.equals("[PlyCount")) {
			  String count = itr.nextToken();
			  count = count.substring(1, count.length() - 2);
			  plyCount.set(count);
			  DoubleWritable result = new DoubleWritable(100.0 / (double) total);
			  context.write(plyCount, result);
		  }
      }
    }
  }

  public static class FirstReducer
       extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
    private DoubleWritable result = new DoubleWritable();

    public void reduce(Text key, Iterable<DoubleWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      double sum = 0.0;
      for (DoubleWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static class SecondMapper
       extends Mapper<Object, Text, DoubleWritable, Text>{

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
		StringTokenizer itr = new StringTokenizer(value.toString());
		String token1 = itr.nextToken();
		String token2 = itr.nextToken();

		DoubleWritable outputKey = new DoubleWritable(Double.parseDouble(token2) * (-1));
		Text outputValue = new Text(token1);
		context.write(outputKey, outputValue);
    }
  }

  public static class SecondReducer
       extends Reducer<DoubleWritable, Text, Text, Text> {

    public void reduce(DoubleWritable key, Iterable<Text> values, Context context
                       ) throws IOException, InterruptedException {
      for (Text val : values) {
		  Text newKey = val;
		  Text newVal = new Text(key.get()*(-1) + "%");
		  context.write(newKey, newVal);
      }
    }
  }

  public static void main(String[] args) throws Exception {

	final String OUTPUT_PATH = "chessdata/intermediate_output";

    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "task C1");
    job.setJarByClass(ChessDataC.class);
    job.setMapperClass(FirstMapper.class);
    job.setCombinerClass(FirstReducer.class);
    job.setReducerClass(FirstReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
	job.waitForCompletion(true);


	Configuration conf2 = new Configuration();	
    Job job2 = Job.getInstance(conf2, "task C2");
    job2.setJarByClass(ChessDataC.class);
    job2.setMapperClass(SecondMapper.class);
    job2.setReducerClass(SecondReducer.class);
    job2.setOutputKeyClass(DoubleWritable.class);
    job2.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job2, new Path(OUTPUT_PATH));
    FileOutputFormat.setOutputPath(job2, new Path(args[1]));

    System.exit(job2.waitForCompletion(true) ? 0 : 1);
  }
}
