
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
import org.json.JSONObject;
import org.apache.hadoop.io.DoubleWritable;

public class RedditAverage extends Configured implements Tool {

	public static class TokenizerMapper
	extends Mapper<LongWritable, Text, Text, LongPairWritable>{

		private final static long numCmt = 1;
        private Text subredditName = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context
				) throws IOException, InterruptedException {

            JSONObject record = new JSONObject(value.toString()); // parsing JSON
            LongPairWritable pair = new LongPairWritable();       
			
            subredditName.set(record.get("subreddit").toString());
			long sumScore = Long.parseLong(record.get("score").toString());
			pair.set(numCmt,sumScore);
            context.write(subredditName, pair);
		}
	}
	
    public static class RedditReducer
	extends Reducer<Text, LongPairWritable, Text, DoubleWritable> {
		private DoubleWritable result = new DoubleWritable();

		@Override
		public void reduce(Text key, Iterable<LongPairWritable> values,
				Context context) throws IOException, InterruptedException {

			double redditSumScore = 0;
            int numRedditCmts = 0;
            double redditAvgScore = 0;       
			for (LongPairWritable val : values) {
				redditSumScore += val.get_1();
                numRedditCmts += val.get_0();
			}
            redditAvgScore = redditSumScore/numRedditCmts;
			result.set(redditAvgScore);
			context.write(key, result);
		}
	}
	
    public static class RedditCombiner
    extends Reducer<Text, LongPairWritable, Text, LongPairWritable> {

        private LongPairWritable result = new LongPairWritable();

        @Override
        public void reduce(Text key, Iterable<LongPairWritable> values,
				Context context
				) throws IOException, InterruptedException {
            int numRedditCmts = 0;
			long sumRedditCmts = 0;
            for (LongPairWritable val : values) {
                sumRedditCmts += val.get_1();
				numRedditCmts += val.get_0();
            }
			result.set(numRedditCmts, sumRedditCmts);
			context.write(key,result);
        }
    }

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new RedditAverage(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, "reddit average");
		job.setJarByClass(RedditAverage.class);

		job.setInputFormatClass(TextInputFormat.class);

		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(RedditCombiner.class); 
		job.setReducerClass(RedditReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongPairWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		TextInputFormat.addInputPath(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
    }
}
