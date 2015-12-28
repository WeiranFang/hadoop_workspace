import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ChessDataA {

  public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{

      private final static IntWritable one = new IntWritable(1);
      private Text winner = new Text();

      public void map(Object key, Text value, Context context) 
                      throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
          String token = itr.nextToken();
          // Check each cases of different winner
          if (token.equals("0-1")) {
            winner.set("Black");
          } else if (token.equals("1-0")) {
            winner.set("White");
          } else if (token.equals("1/2-1/2")) {
            winner.set("Draw");
          } else {
            continue;
          }
          context.write(winner, one);

          // Save total count into "0000":
          Text totalCount = new Text();
          totalCount.set("0000");
          context.write(totalCount, one);
        }
      }
    }

  public static class SumReducer extends Reducer<Text,IntWritable,Text,Text> {
      //private IntWritable result = new IntWritable();
      private Text result = new Text();

      private long total;

      public void reduce(Text key, Iterable<IntWritable> values, Context context) 
                         throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
          sum += val.get();
        }
        if (key.toString().equals("0000")) {
          // This key will be first passed to reducer
          // Set the sum as total count and will be used in other reducers
          total = sum;
          return;
        }
        // Calculate the rate
        double rate = (double) sum / (double) total;
        result.set(sum + " " + rate);
        context.write(key, result);
      }
    }

  // The combiner is very similar to the one in WordCount
  public static class SumCombiner extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();
    public void reduce(Text key, Iterable<IntWritable> values, 
        Context context) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "chess taskA");
    job.setJarByClass(ChessDataA.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(SumCombiner.class);
    job.setReducerClass(SumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
