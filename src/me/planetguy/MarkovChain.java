package me.planetguy;

import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class MarkovChain extends Configured implements Tool {

	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, MapWritableFancy> {

		static enum Counters { INPUT_WORDS }

		private Text lastWord = new Text();
		private Text nextWord = new Text();


		private boolean caseSensitive = true;
		private Set<String> patternsToSkip = new HashSet<String>();

		private long numRecords = 0;
		private String inputFile;

		public void configure(JobConf job) {
			caseSensitive = job.getBoolean("wordcount.case.sensitive", true);
			inputFile = job.get("map.input.file");

			if (job.getBoolean("wordcount.skip.patterns", false)) {
				Path[] patternsFiles = new Path[0];
				try {
					patternsFiles = DistributedCache.getLocalCacheFiles(job);
				} catch (IOException ioe) {
					System.err.println("Caught exception while getting cached files: " + StringUtils.stringifyException(ioe));
				}
				for (Path patternsFile : patternsFiles) {
					parseSkipFile(patternsFile);
				}
			}
		}

		private void parseSkipFile(Path patternsFile) {
			try {
				BufferedReader fis = new BufferedReader(new FileReader(patternsFile.toString()));
				String pattern = null;
				while ((pattern = fis.readLine()) != null) {
					patternsToSkip.add(pattern);
				}
			} catch (IOException ioe) {
				System.err.println("Caught exception while parsing the cached file '" + patternsFile + "' : " + StringUtils.stringifyException(ioe));
			}
		}
		
		private String token(StringTokenizer tokenizer){
			return tokenizer.nextToken().replaceAll("\\W", "");
		}

		public void map(LongWritable key, Text value, OutputCollector<Text, MapWritableFancy> output, Reporter reporter) throws IOException {
			String processedInput = value.toString().toLowerCase();
			StringTokenizer tokenizer = new StringTokenizer(processedInput);
			
			//First token
			if(tokenizer.hasMoreTokens())
				lastWord.set(token(tokenizer));
			
			//Each next token
			while (tokenizer.hasMoreTokens()) {
				String nextToken = token(tokenizer);
				nextWord.set(nextToken);
				MapWritableFancy map=new MapWritableFancy();
				map.put(nextWord, new DoubleWritable(1));
				output.collect(lastWord, map);
				reporter.incrCounter(Counters.INPUT_WORDS, 1);
				lastWord.set(nextToken);
			}

			if ((++numRecords % 100) == 0) {
				reporter.setStatus("Finished processing " + numRecords + " records " + "from the input file: " + inputFile);
			}
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, MapWritableFancy, Text, MapWritableFancy> {
		public void reduce(Text key, Iterator<MapWritableFancy> values, OutputCollector<Text, MapWritableFancy> output, Reporter reporter) throws IOException {
			MapWritableFancy counts = new MapWritableFancy();
			
			int followedCount=0;
			while (values.hasNext()) {
				for(Writable w:values.next().keySet()){
					Text follower = (Text) w;
					followedCount++;
					if(counts.containsKey(follower)){
						DoubleWritable oldCount=(DoubleWritable) counts.get(follower);
						counts.put(follower, new DoubleWritable(oldCount.get()+1));
					} else {
						counts.put(follower, new DoubleWritable(1));
					}
				}
			}
			MapWritableFancy normalizedCounts=new MapWritableFancy();
			for(Writable w:counts.keySet()){
				DoubleWritable d=(DoubleWritable) counts.get(w);
				normalizedCounts.put(w, new DoubleWritable(d.get()/followedCount));
			}
			output.collect(key, normalizedCounts);
		}
	}

	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), MarkovChain.class);
		conf.setJobName("wordcount");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(MapWritableFancy.class);

		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		List<String> inputFiles = new ArrayList<String>();
		for (int i=1; i < args.length; ++i) {
			inputFiles.add(args[i]);
		}

		FileInputFormat.setInputPaths(conf, String.join(",", inputFiles));
		FileOutputFormat.setOutputPath(conf, new Path(args[0]));

		JobClient.runJob(conf);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new MarkovChain(), args);
		System.exit(res);
	}
}