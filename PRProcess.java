package PageRank;

import java.io.IOException;


import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PRProcess {

	private static final Pattern outLinksPattern = Pattern.compile("\\[\\[.+?\\]\\]");

	public static class PageCountMapper 
	extends Mapper<Object, Text, Text, IntWritable>{

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text("N=");

		public void map(Object key, Text value, Context context
		) throws IOException, InterruptedException {
			context.write(word,one);
		}
	}

	public static class IntSumReducer 
	extends Reducer<Text,IntWritable,Text,IntWritable> {

		public void reduce(Text key, Iterable<IntWritable> values, 
				Context context
		) throws IOException, InterruptedException {
			int sum = 0;
			Iterator<IntWritable> val = values.iterator();
			while(val.hasNext()) {
				sum += val.next().get();      
			}
			context.write(key, new IntWritable(sum));
		}
	}

	public static void calTotalPages(String inputPath, String outputPath) throws Exception {
		Configuration conf = new Configuration();
		Job job3 = new Job(conf, "page_count");
		job3.setJarByClass(PRProcess.class);
		job3.setMapperClass(PageCountMapper.class);
		job3.setCombinerClass(IntSumReducer.class);
		job3.setReducerClass(IntSumReducer.class);
		job3.setNumReduceTasks(1);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job3, new Path(inputPath));
		FileOutputFormat.setOutputPath(job3, new Path(outputPath));
		job3.waitForCompletion(true);
		//System.exit(job3.waitForCompletion(true) ? 0 : 1);
	}

	public static class PageLinkMapReduce extends Mapper<LongWritable, Text, Text, Text> {

	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

	        int start = value.find("<title>");
	        int end = value.find("</title>", start);
	        start += 7;
	        String pageString = Text.decode(value.getBytes(), start, end-start);

			Text page = new Text(pageString.replaceAll("\\s", "_"));
			
			context.write(page, new Text("$"));

	        start = value.find("<text");
			start = value.find(">", start);
			end = value.find("</text>", start);
			start += 1;

			if(start == -1 || end == -1)
				return;


	        Matcher matcher = outLinksPattern.matcher(Text.decode(value.getBytes(), start, end-start));
	        Set<String> repeatHashSet = new HashSet<String>();
	        while (matcher.find()) {
	            String outLinkPage = matcher.group();
	            outLinkPage = getoutLinkPage(outLinkPage);
	            if(outLinkPage == null || outLinkPage.isEmpty())
	                continue;
	            if(repeatHashSet.add(outLinkPage))
	            	context.write(new Text(outLinkPage), page);
	        }
	    }


	    private String getoutLinkPage(String currWikiLink){
	        if(isNotWikiLink(currWikiLink)) return null;
	        int start = currWikiLink.startsWith("[[") ? 2 : 1;
	        int endLink = currWikiLink.indexOf("]");
	        int pipePosition = currWikiLink.indexOf("|");
	        if(pipePosition > 0){
	            endLink = pipePosition;
	        }
	        int part = currWikiLink.indexOf("#");
	        if(part > 0){
	            endLink = part;
	        }
	        currWikiLink = currWikiLink.substring(start, endLink);
	        currWikiLink = currWikiLink.replaceAll("\\s", "_");
	        currWikiLink = currWikiLink.replaceAll(",", "");

	        return currWikiLink;
	    }

	    private boolean isNotWikiLink(String currWikiLink) {
	    	if (currWikiLink == null || currWikiLink.isEmpty())
	    		return true;
	        return false;
	    }
	}

	public static class PageLinkReducerInit extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
				StringBuilder adjList = new StringBuilder("");
				boolean isRedLink = true;
				boolean first = true;
				Iterator<Text> val = values.iterator();
				String currVal;
				while(val.hasNext()){
					currVal = val.next().toString();
					if(currVal.equals("$")){
						isRedLink = false;
						continue;
					}
					else{
						if(!first)
							adjList.append("\t");
						adjList.append(currVal);
						
						first = false;
					}
				}
				if(!isRedLink)
					context.write(key, new Text(adjList.toString()));
		}
	}

	public static void parseXml(String inputPath, String outputPath) throws Exception{
		Configuration conf = new Configuration();
		conf.set("xmlinput.start", "<page>");
		conf.set("xmlinput.end", "</page>");
		Job job1 = new Job(conf,"graphGen");
		job1.setJarByClass(PRProcess.class);
		job1.setMapperClass(PageLinkMapReduce.class);
		job1.setInputFormatClass(XMLInputFormat.class);
		job1.setReducerClass(PageLinkReducerInit.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job1,new Path(inputPath));
		FileOutputFormat.setOutputPath(job1,new Path(outputPath));
		job1.waitForCompletion(true);
		//System.exit(job1.waitForCompletion(true) ? 0 : 1);
	}
	

	public static class InlinkToOutlinkMapper extends Mapper<LongWritable, Text, Text, Text>  {
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] tokens = value.toString().split("\t");

			context.write(new Text(tokens[0]), new Text(""));
			//}
			//else{
			int i = 1;
			while (i < tokens.length) {
				context.write(new Text(tokens[i++]), new Text(tokens[0]));
			}
			//}
		}
	}

	public static class InlinkToOutlinkReducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context)
		        throws IOException, InterruptedException {
				StringBuilder adjList = new StringBuilder("");
				boolean first = true;
				Iterator<Text> val = values.iterator();
				String currVal = "";
				while(val.hasNext()){
					currVal = val.next().toString();
					if(!currVal.equals("")){
						if(!first)
							adjList.append("\t");
						adjList.append(currVal);
						
						first = false;
					}
				}
	
				context.write(key, new Text(adjList.toString()));
		}
	}

	public static void getAdjacencyGraph(String inputPath, String outputPath) throws Exception{
	    Configuration conf = new Configuration();
		Job job2 = new Job(conf,"graphGenOut");
		job2.setJarByClass(PRProcess.class);
		job2.setMapperClass(InlinkToOutlinkMapper.class);
		job2.setReducerClass(InlinkToOutlinkReducer.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job2,new Path(inputPath));
		FileOutputFormat.setOutputPath(job2,new Path(outputPath));
		job2.waitForCompletion(true);
	}	 
}
