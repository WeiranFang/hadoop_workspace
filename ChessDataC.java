import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ChessDataC {

	// Use "####" as a mark for total count, and it will be the first line in the output of the first mapreduce
	//private final static String TOTAL = "####";

	public static class FirstMapper extends Mapper<Object, Text, Text, IntWritable>{

		private final static IntWritable one = new IntWritable(1);
		private Text plyCount = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				String label = itr.nextToken();
				// Find the label with "PlyCount" and write out the value
				if (label.equals("[PlyCount")) {
					String count = itr.nextToken();
					count = count.substring(1, count.length() - 2);
					plyCount.set(count);
					context.write(plyCount, one);
					// For every game, add to total count
					//context.write(new Text(TOTAL), one);
				}
			}
		}
	}

	public static class FirstReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {			
			int sum = 0;
			// Count the sum for every key
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static class SecondMapper extends Mapper<Object, Text, Text, Text>{

		//private int total = 0;
		private Text outputKey = new Text();
		private Text outputVal = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				String step = itr.nextToken();
				int number = Integer.parseInt(itr.nextToken());
				// The first line of the previous mapred job would be the total count,
				// save it for other keys to calculate the percentage
				//if (step.equals(TOTAL)) {
				//	total = number;
				//	break;
				//}
				// Use negative rate as output key for sorting
				//outputKey.set(-100.0 * (double) number / (double) total);
				outputKey.set("dummy");
				outputVal.set(step + " " + number);
				context.write(outputKey, outputVal);
			}
		}
	}

	public static class SecondReducer extends Reducer<Text,Text,Text,Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {	
			TreeMap<Integer, List<String>> map = new TreeMap<Integer, List<String>>();
			long sum = 0;
			for (Text val : values) {
				String[] strs = val.toString().split("\\s+");
				String step = strs[0];
				int number = Integer.parseInt(strs[1]);
				sum += (long) number;
				if (!map.containsKey(number)) {
					List<String> list = new ArrayList<String>();
					list.add(step);
					map.put(number, list);
				} else {
					map.get(number).add(step);
				}
				// Exchange key and value for the final output
				//Text newKey = val;
				//Text newVal = new Text(key.get()*(-1) + "%");
				//context.write(newKey, newVal);
			}
			for (Integer num : map.descendingKeySet()) {
				double rate = (double) num / (double) sum * 100;
				for (String str : map.get(num)) {
					context.write(new Text(str), new Text(rate + "%"));
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		final String OUTPUT_PATH = "s3://weiran.bucket/chess_c/intermediate_output";

		// Begin first mapred job
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "task C1");
	    job.setJarByClass(ChessDataC.class);
	    job.setMapperClass(FirstMapper.class);
	    job.setCombinerClass(FirstReducer.class);
	    job.setReducerClass(FirstReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
		job.waitForCompletion(true);

		//Begin second mapred job
		Configuration conf2 = new Configuration();	
	    Job job2 = Job.getInstance(conf2, "task C2");
	    job2.setJarByClass(ChessDataC.class);
	    job2.setMapperClass(SecondMapper.class);
	    job2.setReducerClass(SecondReducer.class);
	    job2.setOutputKeyClass(Text.class);
	    job2.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job2, new Path(OUTPUT_PATH));
	    FileOutputFormat.setOutputPath(job2, new Path(args[1]));

	    System.exit(job2.waitForCompletion(true) ? 0 : 1);
	}
}
