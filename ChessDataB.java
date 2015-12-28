import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ChessDataB {
	static class IntArrayWritable extends ArrayWritable {
		public IntArrayWritable() {
			super(IntWritable.class);
		}
	}

	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntArrayWritable>{

		private final static IntWritable one = new IntWritable(1);
		private final static IntWritable zero = new IntWritable(0);

		private Text wKey = new Text();
		private Text bKey = new Text();

		public void map(Object key, Text value, 
			Context context) throws IOException, InterruptedException {

			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				String currToken = "";
				while (itr.hasMoreTokens()) {
					currToken = itr.nextToken();
					if (currToken.equals("[White")) break;
				}
				if (!itr.hasMoreTokens()) break;
				String wPlayer = itr.nextToken();
				wPlayer = wPlayer.substring(1, wPlayer.length() - 2);
				wKey.set(wPlayer + " White");
				
				while (itr.hasMoreTokens()) {
					currToken = itr.nextToken();
					if (currToken.equals("[Black")) break;
				}
				if (!itr.hasMoreTokens()) break;
				String bPlayer = itr.nextToken();
				bPlayer= bPlayer.substring(1, bPlayer.length() - 2);	
				bKey.set(bPlayer + " Black");

				IntWritable[] wValues = new IntWritable[3];
				IntWritable[] bValues = new IntWritable[3];
				while (itr.hasMoreTokens()) {
					currToken = itr.nextToken();
					if (currToken.equals("0-1")) {
						wValues[0] = zero;
						wValues[1] = one;
						wValues[2] = zero;
						bValues[0] = one;
						bValues[1] = zero;
						bValues[2] = zero;
						break;
					} else if (currToken.equals("1-0")) {
						wValues[0] = one;
						wValues[1] = zero;
						wValues[2] = zero;
						bValues[0] = zero;
						bValues[1] = one;
						bValues[2] = zero;
						break;
					} else if (currToken.equals("1/2-1/2")) {
						wValues[0] = zero;
						wValues[1] = zero;
						wValues[2] = one;
						bValues[0] = zero;
						bValues[1] = zero;
						bValues[2] = one;
						break;
					} else {
						continue;
					}
				}

				IntArrayWritable wArrayWritable = new IntArrayWritable();
				wArrayWritable.set(wValues);
				IntArrayWritable bArrayWritable = new IntArrayWritable();
				bArrayWritable.set(bValues);
				context.write(wKey, wArrayWritable);
				context.write(bKey, bArrayWritable);
			}
		}
	}

	public static class SumReducer extends Reducer<Text,IntArrayWritable,Text,Text> {
		private Text result = new Text();

		public void reduce(Text key, Iterable<IntArrayWritable> arrays,
				Context context) throws IOException, InterruptedException {
			int win = 0, lose = 0, draw = 0;
			for (IntArrayWritable arrayWritable : arrays) {
				Writable[] array = arrayWritable.get();
				win += ((IntWritable) array[0]).get();
				lose += ((IntWritable) array[1]).get();
				draw += ((IntWritable) array[2]).get();
			}
			long sum = (long) win + (long) lose + (long) draw;
			double winRate = (double) win / (double) sum;
			double loseRate = (double) lose / (double) sum;
			double drawRate = (double) draw / (double) sum;
			result.set(winRate + " " + loseRate + " " + drawRate);
			context.write(key, result);
		}
	}

	public static class SumCombiner extends Reducer<Text,IntArrayWritable,Text,IntArrayWritable> {
		private IntArrayWritable result = new IntArrayWritable();

		public void reduce(Text key, Iterable<ArrayWritable> arrays, Context context) throws IOException, InterruptedException {
			int win = 0, lose = 0, draw = 0;
			for (ArrayWritable arrayWritable : arrays) {
				Writable[] array = arrayWritable.get();
				win += ((IntWritable) array[0]).get();
				lose += ((IntWritable) array[1]).get();
				draw += ((IntWritable) array[2]).get();
			}
			IntWritable[] values = new IntWritable[3];
			values[0] = new IntWritable(win);
			values[1] = new IntWritable(lose);
			values[2] = new IntWritable(draw);
			result.set(values);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("textinputformat.record.delimiter", "Event");
		Job job = Job.getInstance(conf, "chess taskB");
		job.setJarByClass(ChessDataB.class);
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
