package PageRank;

//import Context;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.util.GenericOptionsParser;

public class PageRank {
	//private static Integer numPages;
	private static final String PCNT = "p_cnt";
	private Configuration conf = new Configuration();
	
	private static Integer pageRead(String fileName) {
		Integer count = 0;
		Configuration conf = new Configuration();
		BufferedReader br = null;
		FileSystem fs = null;
		Path path = new Path(fileName);
		try {
			fs = path.getFileSystem(conf);
			br = new BufferedReader(new InputStreamReader(fs.open(path)));
			String line = br.readLine();
			//System.out.println("line "+line);
			if (line != null && !line.isEmpty()) {
				//String s[] = line.split('\t');
				line = line.trim();
				//System.out.println("line "+line);
				String s[] = line.split("\t");
				if(s.length>0)
					count = Integer.parseInt(s[1]);
				else count=0;
				//System.out.println("count "+count);
				return count;
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (br != null)
					br.close();
				if (fs != null)
					fs.close();
			} catch (IOException e) {
				 e.printStackTrace();
			}
		}
		return count;
	}


	public static class PRMapper
	extends Mapper<Object, Text, Text, Text>{

		private Text page = new Text();
		private Text outlinks = new Text();
		private Text outlink_page = new Text();
		private Text templine = new Text();
		public void map(Object key, Text value, Context context) 
		throws IOException, InterruptedException {
			//System.out.println("In Map Start: Value : "+value.toString());
			String tokens[] = value.toString().split("\t");
			String pageOLinks[] = value.toString().split("\t",2);
			// get the page and its page rank
			String firstElem = tokens[0];
			String[] results = firstElem.split(", ");
			String fPage = results[0];
			Double pRank = Double.parseDouble(results[1]);
			int outnum = tokens.length-1;
			for(int i=1;i<=outnum;i++){
				Double newVal = pRank/outnum;
				String temp = Double.toString(newVal);
				templine.set(temp);
				outlink_page.set(tokens[i]);
				context.write(outlink_page, templine);
			}
			page.set(fPage);
			//String removedPR = tokens[1];
			if(pageOLinks.length>0){
				outlinks.set(pageOLinks[1]);
			}
			else{
				outlinks.set("");
			}
			//System.out.println("In Map End : Page: "+page.toString()+" outlinks: "+outlinks.toString());
			context.write(page,outlinks);		  
		}
	}


	public static class PRReducer extends Reducer<Text, Text, Text, Text> {

		private Text nkey = new Text();
		private Text outlinks = new Text();
		public void reduce(Text key, Iterable<Text> values, Context context) 
		throws IOException, InterruptedException {
			String olinks="";
			
			double tpages = Double.parseDouble(context.getConfiguration().get(PCNT));
			Double rank = (0.15d/tpages);
			Double sum = 0.0;
			//System.out.println("-------------Key----------"+key.toString());
			//System.out.println("rank ");
			for(Text line : values){
				String s= line.toString();
				//System.out.println(" In Reduce : s :"+s);
				//String tokens[] = s.split(" ");
				
				Double old;
				try{
					old = Double.parseDouble(s);
					//old = Double.parseDouble(tokens[tokens.length-1]);
					sum+=old;
					//System.out.println("old: "+old+" sum: "+sum);
				}
				// If the value is not a pagerank value, then it is outlink list
				catch(NumberFormatException nfe){
					//emit pagek,rankK and its outlinks
					//String totalRank = ", "+Float.toString(rank);
					//olinks.set(totalRank+"\t"+line);
					olinks = s;						
				}
			}
			//System.out.println("rank before 0.85 "+rank);
			rank=rank+(sum*0.85d);
			//System.out.println("rank after 0.85 "+rank);
			//System.out.println("-------------Key----------"+key.toString());
			String key1 = key.toString();
			key1 += ", "+Double.toString(rank);
			
			nkey.set(key1);
			outlinks.set(olinks);
			context.write(nkey, outlinks);
		}
	}


	public static class InitMapper 
	extends Mapper<Object, Text, Text, Text>{

		//private final static IntWritable one = new IntWritable(1);
		//private Text word = new Text();
		private Text line = new Text();
		private Text rest = new Text();

		public void map(Object key, Text value, Context context
		) throws IOException, InterruptedException {
			// Split the string into 2 parts.
			
			String tokens[] = value.toString().split("\t", 2);
			//System.out.println(" token[0] :"+tokens[0]+" token[1] :"+tokens[1]);
			double tpages = Double.parseDouble(context.getConfiguration().get(PCNT));
			//System.out.println("numPages1 :"+tpages);
			if(tokens.length>0){
				//System.out.println("numPages2 :"+tpages);
				if(!tokens[0].equals("")){
					//System.out.println("numPages3 :"+tpages);
					line.set(tokens[0]+", "+1.0/tpages);
					//System.out.println(" in line.set :"+tokens[0]+", "+1.0/tpages);
				}
				else{
					line.set("");
				}
				if(!tokens[1].equals("")){
					rest.set(tokens[1]);
					//System.out.println(" in rest.set :"+tokens[1]);
				}
				else{
					rest.set("");
				}
				context.write(line, rest);
			}
		}
	}

	public static class InitReducer 
	extends Reducer<Object, Text, Text, Text>{
		public void reduce(Text key, Iterable<Text> values, Context context) 
		throws IOException, InterruptedException {

		}
	}

	public static class PrintMapper
	extends Mapper<Object, Text, DoubleWritable, Text>{
		private Text title = new Text();
		private DoubleWritable pr = new DoubleWritable();
		public void map(Object key, Text value, Context context) 
		throws IOException, InterruptedException {
			//System.out.println("-----Key-----"+key.toString());
			//System.out.println("-----Value-----"+value.toString());
			String token[] = value.toString().split(", ");
			String ptitle  = token[0];
			double tpages = Double.parseDouble(context.getConfiguration().get(PCNT));
			if(token.length>1){
				String temp[] = token[1].split("\t");
				//if(temp.length>0){
				if(Double.parseDouble(temp[0])>=(5.0/tpages)){
					double p = -1.0*Double.parseDouble(temp[0]);
					//String prank = Double.toString(p);
					title.set(ptitle); 
					pr.set(p);
					context.write(pr, title);
				}
				//}
			}
		}
	}


	public static class PrintReducer extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {

		//private Text title = new Text();
		private DoubleWritable pr = new DoubleWritable();
		public void reduce(DoubleWritable key, Iterable<Text> values, Context context) 
		throws IOException, InterruptedException {
			//String prank = key.toString();
			double p = -1.0*Double.parseDouble(key.toString());
			//String prank = Double.toString(p);
			//String ptitle = "";
			//System.out.println("-----key---- :"+key.toString());
			for(Text t : values){
				pr.set(p);
				//title.set(ptitle);
				context.write(t,pr);
				//ptitle+=t.toString();
			}
			//System.out.println("-----value---- :"+ptitle);		  
		}
	}

	public static void runPR(String inDir, String outDir, String cpath) throws Exception{
		Configuration conf = new Configuration();
		Integer	numPages = pageRead(cpath);
		//System.out.println("in runPR :"+numPages);
		conf.set(PCNT, numPages.toString());
		Job prjob = new Job(conf, "pRank");
		prjob.setJarByClass(PageRank.class);
		prjob.setMapperClass(PRMapper.class);
		//prjob.setCombinerClass(PRReducer.class);
		
		prjob.setReducerClass(PRReducer.class);
		prjob.setOutputKeyClass(Text.class);
		prjob.setOutputValueClass(Text.class);
		//prjob.setNumReduceTasks(0);
		FileInputFormat.addInputPath(prjob, new Path(inDir));
		FileOutputFormat.setOutputPath(prjob, new Path(outDir));

		prjob.waitForCompletion(true);
	}

	public static void initPR(String inDir, String outDir, String cpath) throws Exception{
		Configuration conf = new Configuration(); 
		
		Integer numPages = pageRead(cpath);
		//System.out.println("in initPR :"+numPages);
		conf.set(PCNT, numPages.toString());
		//System.out.println("in initPR :"+numPages);
		Job job = new Job(conf, "prInit");
		job.setJarByClass(PageRank.class);
		job.setMapperClass(InitMapper.class);
		//job.setCombinerClass(InitReducer.class);
		job.setReducerClass(InitReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setNumReduceTasks(0);
		FileInputFormat.addInputPath(job, new Path(inDir));
		FileOutputFormat.setOutputPath(job, new Path(outDir));
		job.waitForCompletion(true);
	}

	public static void printPR(String inDir, String outDir, String cpath) throws Exception{
		Configuration conf = new Configuration();
		Integer numPages = pageRead(cpath);
		//System.out.println("in printPR :"+numPages);
		conf.set(PCNT, numPages.toString());
		//System.out.println("in printPR PCNT:"+PCNT);
		Job printjob = new Job(conf, "printRank");
		printjob.setJarByClass(PageRank.class);
		printjob.setMapperClass(PrintMapper.class);
		//prjob.setCombinerClass(PRReducer.class);
		printjob.setReducerClass(PrintReducer.class);
		printjob.setMapOutputKeyClass(DoubleWritable.class);
		printjob.setMapOutputValueClass(Text.class);
		printjob.setOutputKeyClass(Text.class);
		printjob.setOutputValueClass(DoubleWritable.class);
		printjob.setNumReduceTasks(1);
		FileInputFormat.addInputPath(printjob, new Path(inDir));
		FileOutputFormat.setOutputPath(printjob, new Path(outDir));	    
		printjob.waitForCompletion(true);
	}
    
	private void rename(String inpName) throws Exception{
		FileSystem fs = null;
		Path src,dst;
		try {
			src = new Path(inpName + "/tmp/results/iter/part-r-00000");
			dst = new Path(inpName + "/results/PageRank.outlink.out");
			fs = src.getFileSystem(conf);
			FileUtil.copyMerge(fs, src, fs, dst, false, conf, null);
			fs.rename(src, dst);
		} catch (IOException e) {
			e.printStackTrace();
		}
		// for first iteration
		src = new Path(inpName + "/tmp/results/iter1_sorted/part-r-00000");
		dst = new Path(inpName + "/results/PageRank.iter1.out");
		fs.rename(src, dst);
		// for last iteration
		src = new Path(inpName + "/tmp/results/iter8_sorted/part-r-00000");
		dst = new Path(inpName + "/results/PageRank.iter8.out");
		fs.rename(src, dst);
		// for count 
		src = new Path(inpName + "/tmp/count/part-r-00000");
		dst = new Path(inpName + "/results/PageRank.n.out");
		fs.rename(src, dst);
		fs.close();
	}
	
	public static void main(String[] args) throws Exception {
		
		//String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (args.length != 1) {
			System.err.println("Wrong Inputs");
			System.exit(2);
		}
		else{
			PageRank prank = new PageRank();
			
			args[0]="s3://"+args[0];
			PRProcess.parseXml("s3://spring-2014-ds/data", args[0]+"/tmp/results/out1");
					
//			PRProcess.parseXml(args[0], args[0]+"/tmp/results/out1");
			PRProcess.getAdjacencyGraph(args[0]+"/tmp/results/out1", args[0] + 	"/tmp/results/iter");

			PRProcess.calTotalPages(args[0] + "/tmp/results/iter", args[0] + "/tmp/count");
   
			String cpath = args[0] + "/tmp/count/part-r-00000";
			initPR(args[0] + "/tmp/results/iter",args[0] + "/tmp/results/iter0",cpath);    
			// Calculate page rank for 8 iterations
			for(int i=0;i<8;){
				runPR(args[0] + "/tmp/results/iter"+Integer.toString(i), 
						args[0] + "/tmp/results/iter"+Integer.toString(++i), cpath);
			}
			printPR(args[0] + "/tmp/results/iter1",args[0] + "/tmp/results/iter1_sorted",cpath);
			printPR(args[0] + "/tmp/results/iter8",args[0] + "/tmp/results/iter8_sorted",cpath);

			// change the file names
			prank.rename(args[0]);

		}
		
	}
}