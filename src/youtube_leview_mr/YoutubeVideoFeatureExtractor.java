package youtube_leview_mr;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import DocProcess.CompositeDocSerialize;
import pipeline.CompositeDoc;
import youtube_leview_mr.YoutubeCompositeDocParserMR.Map;
import youtube_leview_mr.YoutubeCompositeDocParserMR.Reduce;

public class YoutubeVideoFeatureExtractor {
    public static class Map extends  Mapper<Object, Text, Text, Text>
    {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		
		@Override
		public void setup(org.apache.hadoop.mapreduce.Mapper.Context context) throws IOException, InterruptedException {
			System.out.println("begin to setup function!");
		}

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException
		{
			//System.out.println("begin to mapper");
		    String line = value.toString();
		    String[] segments = line.split("\t");
		    if (segments.length != 3) {
		    	context.getCounter("custom", "input column error").increment(1);
		    	return;
		    }
		    
		    CompositeDoc compositeDoc = CompositeDocSerialize.DeSerialize(segments[2], context);
		    
		    if (!compositeDoc.sub_src.equals("youtube")) {
		    	context.getCounter("custom", "Not Youtube video").increment(1);
		    	return;
		    }
		    
		    /*StringBuilder sb = new StringBuilder();
		    sb.append(compositeDoc.title);
		    sb.append('\t');
		    
		    sb.append(compositeDoc.author.url);
		    sb.append('\t');
		    
		    if (compositeDoc.description != null && !compositeDoc.description.isEmpty()) {
		    	sb.append(compositeDoc.description.replace('\t', ' '));
		    }*/
		    
		    context.write(new Text(compositeDoc.author.url), new Text(compositeDoc.title.replace('\t', ' ').replace('\n', ' ').replace('\r', ' ') ));

		}
    }
	public static class Reduce extends Reducer<Text, Text, Text, Text>
	{
		@Override
		public void reduce(Text key, Iterable<Text> values,Context context)
				throws IOException, InterruptedException
		{
			String key_str = key.toString();
			StringBuilder sb = new StringBuilder();
			for (Text text : values) {
				sb.append(text.toString());
				sb.append(". ");
			}
			context.write(new Text(key_str), new Text(sb.toString()));
		}
	}
    
	public static class VideoExtractMap extends  Mapper<Object, Text, Text, Text>
	{
		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException
		{
			//System.out.println("begin to mapper");
		    String line = value.toString();
		    String[] segments = line.split("\t");
		    if (segments.length != 3) {
		    	context.getCounter("custom", "input column error").increment(1);
		    	return;
		    }
		    
		    CompositeDoc compositeDoc = CompositeDocSerialize.DeSerialize(segments[2], context);
		    
		    if (!compositeDoc.sub_src.equals("youtube")) {
		    	context.getCounter("custom", "Not Youtube video").increment(1);
		    	return;
		    }
		    
		    StringBuilder sb = new StringBuilder();
		    sb.append(compositeDoc.title.replace('\t', ' ').replace('\n', ' ').replace('\r', ' ').toLowerCase());
		    sb.append(' ');
		    if (compositeDoc.description != null && !compositeDoc.description.isEmpty()) {
		    	sb.append(compositeDoc.description.replace('\t', ' ').replace('\n', ' ').replace('\r', ' ').toLowerCase());
		    }
		    
		    context.write(new Text(compositeDoc.url), 
		    		new Text(sb.toString()));
		    //context.write(new Text(segments[0]), new Text(compositeDoc.url));

		}		
	}	
	public static class VideoUniqueReduce extends Reducer<Text, Text, Text, Text>
	{
		@Override
		public void reduce(Text key, Iterable<Text> values,Context context)
				throws IOException, InterruptedException
		{
			String key_str = key.toString();
			for (Text text : values) {
				context.write(key, text);
				return;
			}
			
		}
	}
	
	public static class VideoForwardIndexMap extends Mapper<Object, Text, Text, Text> {
		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException
		{
			//System.out.println("begin to mapper");
		    String line = value.toString();
		    String[] segments = line.split("\t");
		    if (segments.length != 3) {
		    	context.getCounter("custom", "input column error").increment(1);
		    	return;
		    }
		    
		    CompositeDoc compositeDoc = CompositeDocSerialize.DeSerialize(segments[2], context);
		    
		    if (!compositeDoc.sub_src.equals("youtube")) {
		    	context.getCounter("custom", "Not Youtube video").increment(1);
		    	return;
		    }
		    
		    StringBuilder sb = new StringBuilder();
		    int total_count = 0;
		    HashMap<String, Integer> words = new HashMap<String, Integer>();
		    for (String word : compositeDoc.title.toLowerCase().split("[,\\.?!:;\\| \t\r\n]")) {
		    	if (words.containsKey(word)) {
		    		words.put(word, words.get(word) + 1);
		    	} else {
		    		words.put(word, 1);
		    	}
		    	total_count ++;
		    }
		    if (compositeDoc.description != null && !compositeDoc.description.isEmpty()) {
			    for (String word : compositeDoc.description.toLowerCase().split("[,\\.?!:;\\| \t\r\n]")) {
			    	if (words.containsKey(word)) {
			    		words.put(word, words.get(word) + 1);
			    	} else {
			    		words.put(word, 1);
			    	}
			    	total_count ++;
			    }
		    }
		    Iterator<Entry<String, Integer>> iter = words.entrySet().iterator();
		    sb.append(total_count);
		    sb.append('\t');
		    while(iter.hasNext()) {
		    	Entry<String, Integer> entry = iter.next(); 
		    	String temp = entry.getKey();
		    	temp = temp.replace(':', '|').replace(',', '|');
		    	
		    	sb.append(temp);
		    	sb.append(':');
		    	sb.append(entry.getValue());
		    	sb.append(',');
		    }
		    
		    context.write(new Text(compositeDoc.url), 
		    		new Text(sb.toString()));
		}			
	}
	
	public static void main(String[] args) throws Exception
    {
    	Configuration conf = new Configuration();
    	conf.set("mapreduce.map.memory.mb", "5000");
    	conf.set("mapreduce.map.java.opts", "-Xmx4608m");
        String[] libjarsArr = args[2].split(",");
        for (int i = 0; i < libjarsArr.length; ++i) {
        	addTmpJar(libjarsArr[i], conf);
        }
    	Job job = new Job(conf, "YoutubeParserMR");
    	job.setJarByClass(YoutubeVideoFeatureExtractor.class);
    	job.setMapperClass(VideoForwardIndexMap.class);
    	job.setReducerClass(VideoUniqueReduce.class);
    	job.setNumReduceTasks(100);
    	job.setOutputKeyClass(Text.class);
    	job.setOutputValueClass(Text.class);

    	
    	FileInputFormat.addInputPath(job, new Path(args[0]));
    	FileOutputFormat.setOutputPath(job, new Path(args[1]));

    	System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
    
	/**
	 * ΪMapreduce��ӵ�����jar��
	 * 
	 * @param jarPath
	 *            ������D:/Java/new_java_workspace/scm/lib/guava-r08.jar
	 * @param conf
	 * @throws IOException
	 */
	public static void addTmpJar(String jarPath, Configuration conf) throws IOException {
		System.setProperty("path.separator", ":");
		FileSystem fs = FileSystem.getLocal(conf);
		String newJarPath = new Path(jarPath).makeQualified(fs).toString();
		String tmpjars = conf.get("tmpjars");
		if (tmpjars == null || tmpjars.length() == 0) {
			conf.set("tmpjars", newJarPath);
		} else {
			conf.set("tmpjars", tmpjars + "," + newJarPath);
		}
	}
}
