	import java.io.IOException;
	import java.util.*;
	import org.apache.hadoop.fs.Path;
	import org.apache.hadoop.conf.*;
	import org.apache.hadoop.io.*;
	import org.apache.hadoop.mapred.*;
	import org.apache.hadoop.util.*;
	
	public class SpecificWordCount {
	
	   public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
	     private final static IntWritable one = new IntWritable(1);
	     private final static IntWritable zero = new IntWritable(0);
	     private final static String wordsToCheck = "won,wins,win,threat,lost,war,defence,failure,dead,liars,fake,like,accepting,bad,protest"; //Enter all the words you want to look for here separated by comma
	     private Text word = new Text();

	     public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
	       String line = value.toString();
	       StringTokenizer tokenizer_input = new StringTokenizer(line,"\n \t \r \f : -");
	       StringTokenizer tokenizer_wordCheck = new StringTokenizer(wordsToCheck,",");
	       int numberOfWords=tokenizer_wordCheck.countTokens();
	       String inputArr[]=new String[numberOfWords];
	       
	       for (int i=0;i<numberOfWords;i++)
	    	   inputArr[i]=tokenizer_wordCheck.nextToken();
	       
	       String temp;
	       while (tokenizer_input.hasMoreTokens()) 
		   {  temp=tokenizer_input.nextToken();
	    	  for (int i=0;i<numberOfWords;i++)
	    	  {		if (temp.equalsIgnoreCase(inputArr[i]))
	    	  	   {word.set(inputArr[i]);
	    	   	   output.collect(word, one);
	    		   }
					else 
				   { word.set(inputArr[i]);
					 output.collect(word, zero);
					}
	    		}   
         	}
	    }
	}
	
	   public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
	     public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
       int sum = 0;
	       while (values.hasNext()) {
	         sum += values.next().get();
	       }
	       output.collect(key, new IntWritable(sum));
	     }
	   }
	
	   public static void main(String[] args) throws Exception {
	     JobConf conf = new JobConf(SpecificWordCount.class);
		 conf.setJobName("specificwordcount");

	     conf.setOutputKeyClass(Text.class);
	     conf.setOutputValueClass(IntWritable.class);
	
	     conf.setMapperClass(Map.class);
	     conf.setCombinerClass(Reduce.class);
	     conf.setReducerClass(Reduce.class);
	
	     conf.setInputFormat(TextInputFormat.class);
	     conf.setOutputFormat(TextOutputFormat.class);
	
	     FileInputFormat.setInputPaths(conf, new Path(args[0]));
	     FileOutputFormat.setOutputPath(conf, new Path(args[1]));
	
	     JobClient.runJob(conf);
	   }
	}
