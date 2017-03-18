package org.myorg;
        

import java.util.*;
import java.net.URI;
import java.io.*;
import java.lang.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.StringUtils;
        
public class Preprocessing {

 public static enum customCounters { LINE_COUNT };
        
 public static class Map extends Mapper<LongWritable, Text, LongWritable, Text> {

    private String input;
    private Set<String> patternsToSkip = new HashSet<String>();
    private int line_number = 0;

    protected void setup(Mapper.Context context)
        throws IOException,
        InterruptedException {
      if (context.getInputSplit() instanceof FileSplit) {
        this.input = ((FileSplit) context.getInputSplit()).getPath().toString();
      } else {
        this.input = context.getInputSplit().toString();
      }

      Configuration config = context.getConfiguration();
      if (config.getBoolean("wordcount.skip.patterns", false)) {
        URI[] localPaths = context.getCacheFiles();
        parseSkipFile(localPaths[0]);
      }
    }

    private void parseSkipFile(URI patternsURI) {
      try {
        BufferedReader fis = new BufferedReader(new FileReader(new File(patternsURI.getPath()).getName()));
        String pattern;
        while ((pattern = fis.readLine()) != null) {
          patternsToSkip.add(pattern);
        }
      } catch (IOException ioe) {
        System.err.println("Caught exception while parsing the cached file '"
            + patternsURI + "' : " + StringUtils.stringifyException(ioe));
      }
    }
    
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();

        if(!line.isEmpty()){
            String[] words = line.split("[^A-Za-z0-9]");

            Set<String> written = new HashSet<String>();

            line_number ++;
            context.getCounter(customCounters.LINE_COUNT).increment(1);

            for (String word : words) {
                word = word.toLowerCase();
                if (word.isEmpty() || patternsToSkip.contains(word) || written.contains(word)) {
                    continue;
                }
                written.add(word);
                context.write(new LongWritable(line_number), new Text(word));
            }
        }
    }
 } 
        
 public static class Reduce extends Reducer<LongWritable, Text, LongWritable, Text> {

    java.util.HashMap<String, Integer> wordcount = new java.util.HashMap<String, Integer>();

    public void setup(Context context) throws IOException, InterruptedException {

        FileSystem fs = FileSystem.get(URI.create("wordcount.csv"), context.getConfiguration());
        Scanner wc = new Scanner(fs.open(new Path("wordcount.csv")));

        while (wc.hasNext()){
            String word_count[] = wc.next().toString().split(",");
            wordcount.put(word_count[0], Integer.parseInt(word_count[1]));
        }
        wc.close();
    }

    public void reduce(LongWritable key, Iterable<Text> values, Context context) 
      throws IOException, InterruptedException {

        List<String> sortedLine = new ArrayList<String>();

        for(Text word : values) {

            String toInsert = word.toString();
            Integer index = new Integer(0);
            Integer frequency = wordcount.get(toInsert);
            Integer len = sortedLine.size();

            for (int i = 0; i < len; i++){
                Integer indexFrequency = wordcount.get(sortedLine.get(i));
                try
                {
                    if (frequency <= indexFrequency){
                    index++;
                }
                }
                catch (NullPointerException nullPointer)
                {
                   System.out.println(sortedLine.get(i));
                }
                
            }

            sortedLine.add(Math.min(index + 1, len), toInsert);
        }

        String mergedLine = String.join(" ", sortedLine);
        context.write(key, new Text(mergedLine));
        
    }    
 }
        
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
        
    Job job = new Job(conf, "Preprocessing");
    job.setJarByClass(Preprocessing.class);

    for (int i = 0; i < args.length; i++) {
      if ("-skip".equals(args[i])) {
        job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
        i += 1;
        job.addCacheFile(new Path(args[i]).toUri());
      }
    }

    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(Text.class);
        
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
    job.waitForCompletion(true);

    FileSystem fs = FileSystem.get(URI.create("line_count.txt"), conf);
    FSDataOutputStream txtFile = fs.create(new Path("line_count.txt"));
    txtFile.writeBytes("line count : " + job.getCounters().findCounter(customCounters.LINE_COUNT).getValue());
    txtFile.close();
    fs.close();

 }
        
}