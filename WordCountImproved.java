// adapted from https://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html
// adated from https://coursys.sfu.ca/2023fa-cmpt-732-g1/pages/Assign1_WordCount

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;
import java.util.*;  
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.regex.Pattern;
import javax.naming.Context;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;

public class WordCountImproved extends Configured implements Tool {

	public static class TokenizerMapper
	extends Mapper<LongWritable, Text, Text, LongWritable>{

		private final static LongWritable one = new LongWritable(1);
		//private List<Text> word = new ArrayList<Text>();

		private Pattern word_sep = Pattern.compile("[\\p{Punct}\\s]+");
		private Text word = new Text();
		
		@Override
		public void map(LongWritable key, Text value, 
		Context context) throws IOException, InterruptedException {
			//String splitWord = value.toString();
		//	String[] splitted = splitWord.split(word_sep.toString());
			String[] splitted = word_sep.split(value.toString());

			//word = (value.split(word_sep)).toLowerCase();
			 for (String aWord : splitted){
			 	aWord = aWord.toLowerCase(); // avoid cases
			 	if (aWord.length() > 0){  // avoid length of 0 word
			 		word.set(aWord); // convert String to Writable
					context.write(word, one);
				}
			 }
			 
			// StringTokenizer itr = new StringTokenizer(value.toString());
			// while (itr.hasMoreTokens()) {
			// 	word.set(itr.nextToken());
			// 	context.write(word, one);
			// }
			
			
		}
	}

    /* 
    public static class IntSumReducer
	extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		@Override
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context
				) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}
    */
	

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new WordCountImproved(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(WordCountImproved.class);

		job.setInputFormatClass(TextInputFormat.class);

		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(LongSumReducer.class); // use the pre-made LongSumReducer instead
		job.setReducerClass(LongSumReducer.class); // use the pre-made LongSumReducer instead

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		TextInputFormat.addInputPath(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
    }
}
