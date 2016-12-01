package youtube_leview_mr;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

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
import DocProcessClassification.DataAdapter.ClassifierInputAllNLPAdapter;
import DocProcessClassification.DataAdapter.ClassifierInputTarget;
import DocProcessClassification.PatternMatch.URLPrefixPatternMatch;
import pipeline.CompositeDoc;

public class YoutubeCompositeDocParserMR {
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
		    
		    StringBuilder sb = new StringBuilder();
		    sb.append(compositeDoc.title);
		    sb.append('\t');
		    
		    sb.append(compositeDoc.author.url);
		    sb.append('\t');
		    
		    if (compositeDoc.description != null && !compositeDoc.description.isEmpty()) {
		    	sb.append(compositeDoc.description.replace('\t', ' '));
		    }
		    
		    context.write(new Text(segments[0]), new Text(sb.toString()));

		}
    }
	public static class Reduce extends Reducer<Text, Text, Text, Text>
	{
		@Override
		public void reduce(Text key, Iterable<Text> values,Context context)
				throws IOException, InterruptedException
		{
			for (Text text : values) {
				context.write(key, text);
			}
		}
	}
    public static void main(String[] args) throws Exception
    {
    	Configuration conf = new Configuration();
    	
    	conf.set("type", "classifier_data");
    	conf.set("mapreduce.map.memory.mb", "5000");
    	conf.set("mapreduce.map.java.opts", "-Xmx4608m");
    	
    	/*String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    	if (otherArgs.length != 2) {
    		System.err.println("Usage: wordcount <in> <out>");
    		System.exit(2);
    	}*/
        String[] libjarsArr = args[2].split(",");
        for (int i = 0; i < libjarsArr.length; ++i) {
        	addTmpJar(libjarsArr[i], conf);
        }
    	Job job = new Job(conf, "YoutubeParserMR");
    	job.setJarByClass(YoutubeCompositeDocParserMR.class);
    	job.setMapperClass(Map.class);
    	//job.setCombinerClass(Reduce.class);
    	job.setReducerClass(Reduce.class);
    	job.setNumReduceTasks(0);
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
