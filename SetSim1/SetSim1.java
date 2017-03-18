package org.myorg;
        

import java.util.*;
import java.net.URI;
import java.io.*;
import java.lang.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.StringUtils;
        
public class SetSim1 {

 public static enum customCounters { LINE_COUNT };
        
 public static class Map extends Mapper<LongWritable, Text, Text, Text> {

    private int lineCount;
    private int Nmax = 20; // last line of the sample to reduce calculation time

    protected void setup(Mapper.Context context)
        throws IOException, InterruptedException {

        FileSystem fs = FileSystem.get(URI.create("line_count.txt"), context.getConfiguration());
        Scanner wc = new Scanner(fs.open(new Path("line_count.txt")));

        String line_count = wc.nextLine().replaceAll("[^0-9.]", "");
        wc.close();

        lineCount = Math.min(Integer.parseInt(line_count), Nmax);
        
    }
    
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] line = value.toString().split("\t");
        if(key.get() < lineCount){
            for (int i=1; i < lineCount; i++){
                if (key.get() != i){
                    Text new_key = new Text(Long.toString(Math.min(key.get(), i)) + "," +  Long.toString(Math.max(key.get(), i)));
                    context.write(new_key, new Text(line[1]));
                }   

            }
        }   
    }
 } 
        
 public static class Reduce extends Reducer<Text, Text, Text, Text> {

    private double threshold = 0.8;

    public void reduce(Text key, Iterable<Text> values, Context context) 
      throws IOException, InterruptedException {

        List<Text> lines = new ArrayList<Text>();
        for (Text val : values) {
            lines.add(val);
        }
        try
        {
            double jac = jaccard(lines.get(0),lines.get(1));
            if (jac >= threshold){
                context.write(key, new Text(Double.toString(jac)));
            }
        }
        catch (IndexOutOfBoundsException nullPointer)
            {
                System.out.println(key);
            }

    }

    private double jaccard(Text s1, Text s2){

        HashSet<String> u = new HashSet<String>(Arrays.asList(s1.toString().split(" ")));
        HashSet<String> v = new HashSet<String>(Arrays.asList(s2.toString().split(" ")));

        HashSet<String> intersection = new HashSet<String>();
        HashSet<String> union = new HashSet<String>();

        if (u.size() >= v.size()){

            intersection = v;
            intersection.retainAll(u);
            union = u;
            union.addAll(v);

        }
        else{

            intersection = u;
            intersection.retainAll(v);
            union = v;
            union.addAll(u);
        }

        return (double) intersection.size() / union.size();

    }
 }
        
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
        
    Job job = new Job(conf, "SetSim1");
    job.setJarByClass(SetSim1.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
        
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
    job.waitForCompletion(true);

 }


//----------------- I tried to implement a custom input format, it didn't work.

// public class CustomInputFormat implements FileInputFormat<LongWritable, Text> {

//         public CustomInputFormat(){}

//         public CustomInputFormat(Object... o) {}

//         @Override
//         public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
//             return new CustomRecordReader();
//         }
//     }

// private class CustomRecordReader implements RecordReader<LongWritable, Text> {
 
//         private LineRecordReader lineReader;
//         private LongWritable lineKey;
//         private Text lineValue;
       
//         @Override
//         public void initialize(InputSplit split, TaskAttemptContext context)
//                 throws IOException, InterruptedException {
//             lineReader.initialize(split, context);
//             lineKey = lineReader.createKey();
//             lineValue = lineReader.createValue();
//         }

//         public boolean next(LongWritable key, Text value) throws IOException {

//             if (!lineReader.next(lineKey, lineValue)) {
//               return false;
//             }

//             String [] pieces = lineValue.toString().split("\t");

//             key = new LongWritable(Long.parseLong(pieces[0]));
//             value = new Text(pieces[1]);

//             return true;
//         }
 
//         @Override
//         public boolean nextKeyValue() throws IOException, InterruptedException {
//             return lineReader.nextKeyValue();
//         }
 
//         @Override
//         public LongWritable getCurrentKey() throws IOException, InterruptedException {
//             return lineReader.getCurrentValue();
//         }
 
//         @Override
//         public Text getCurrentValue() throws IOException, InterruptedException {
//             return new Text();
//         }
 
//         @Override
//         public float getProgress() throws IOException, InterruptedException {
//             return lineReader.getProgress();
//         }
 
//         @Override
//         public void close() throws IOException {
//             lineReader.close();
//         }
//     }

}     
