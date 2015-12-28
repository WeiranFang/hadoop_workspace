import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ChessDataA {

  public static class IntArrayWritable extends ArrayWritable {
    public IntArrayWritable() {
      super(IntWritable.class);
    }
  }

  public static class TokenizerMapper extends Mapper<Object, Text, Text, IntArrayWritable>{

      private final static IntWritable one = new IntWritable(1);
      private final static IntWritable zero = new IntWritable(0);

      private Text dummyKey = new Text("dummy");

      public void map(Object key, Text value, Context context) 
                      throws IOException, InterruptedException {

        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
          String token = itr.nextToken();
          IntArrayWritable arrayWritable = new IntArrayWritable();
          // values for [Black, White, Draw]
          IntWritable[] values = new IntWritable[3];           

          // Check different cases
          if (token.equals("0-1")) {
            values[0] = one;
            values[1] = zero;
            values[2] = zero;
          } else if (token.equals("1-0")) {
            values[0] = zero;
            values[1] = one;
            values[2] = zero;
          } else if (token.equals("1/2-1/2")) {
            values[0] = zero;
            values[1] = zero;
            values[2] = one;
          } else {
            continue;
          }
          arrayWritable.set(values);
          context.write(dummyKey, arrayWritable);
        }
      }
    }

  public static class SumReducer extends Reducer<Text,IntArrayWritable,Text,Text> {
      //private IntWritable result = new IntWritable();
      private Text result1 = new Text();
      private Text result2 = new Text();
      private Text result3 = new Text();

      public void reduce(Text key, Iterable<IntArrayWritable> arrays, Context context) 
                         throws IOException, InterruptedException {
        // For each case, count the sum respectfully
        int w = 0, b = 0, d = 0;
        for (IntArrayWritable arrayWritable : arrays) {
          Writable[] array = arrayWritable.get();
          b += ((IntWritable) array[0]).get();
          w += ((IntWritable) array[1]).get();
          d += ((IntWritable) array[2]).get();
        }
        long total = (long) w + (long) b + (long) d;

        // Calculate the rate for each case
        double brate = (double) b / (double) total;
        double wrate = (double) w / (double) total;
        double drate = (double) d / (double) total;

        result1.set(b + " " + brate);
        result2.set(w + " " + wrate); 
        result3.set(d + " " + drate); 

        // Write output result
        context.write(new Text("Black"), result1);
        context.write(new Text("White"), result2);
        context.write(new Text("Draw"), result3);
      }
    }

  // The combiner is very similar to the one in WordCount
  public static class SumCombiner extends Reducer<Text,IntArrayWritable,Text,IntArrayWritable> {
    private IntArrayWritable result = new IntArrayWritable();

    public void reduce(Text key, Iterable<ArrayWritable> arrays, 
        Context context) throws IOException, InterruptedException {
      int w = 0, b = 0, d = 0;
      for (ArrayWritable arrayWritable : arrays) {
        Writable[] array = arrayWritable.get();
        b += ((IntWritable) array[0]).get();
        w += ((IntWritable) array[1]).get();
        d += ((IntWritable) array[2]).get();
      }
      IntWritable[] values = new IntWritable[3];
      values[0] = new IntWritable(b);
      values[1] = new IntWritable(w);
      values[2] = new IntWritable(d);
      result.set(values);
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
    job.setOutputValueClass(IntArrayWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
