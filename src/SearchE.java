import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class SearchE extends Configured implements Tool {

    private static final Logger LOG = Logger.getLogger(Search.class);
    public static String query = "";

    public static void main(String[] args) throws Exception {

        BufferedReader input=new BufferedReader(new InputStreamReader(System.in));
        System.out.println("Enter query: ");
        query = input.readLine();

        int res = ToolRunner.run(new Search(), args); // running ToolRunner

        System.exit(res);
    }

    public int run(String[] args) throws Exception {

        int result = 0;
        Job job = Job.getInstance(getConf(), " Search "); //creating job
        job.setJarByClass(this.getClass());  //creating jar

        Configuration conf = new Configuration();
        FileInputFormat.addInputPaths(job, args[0]); //input file path in HDFS for TFIDF
        FileOutputFormat.setOutputPath(job, new Path(args[1])); //output file path in HDFS
   

        
        job.setJarByClass(this.getClass());
        job.setMapperClass(Map.class); //initialization of mapper class
        job.setReducerClass(Reduce.class); //initialization of reducer class
        job.setOutputKeyClass(Text.class); //text object to output key
        job.setOutputValueClass(Text.class);  // value represents number of times that word appears

         return job.waitForCompletion(true)  ? 0 : 1; //launching the job
    }

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable offset, Text lineText, Context context)
                throws IOException, InterruptedException {

            String line = lineText.toString();

            String word = line.substring(0, line.indexOf("#"));    //retrieving word

            String fileName = line.substring(line.lastIndexOf("#") + 1, line.lastIndexOf("\t")); //retrieving file name

            String termFrequency = line.substring(line.lastIndexOf("\t") + 1); //retrieving term frequency
         
            Configuration config = context.getConfiguration();
            
            StringTokenizer st = new StringTokenizer(query); //breaking string into tokens

            while (st.hasMoreTokens()) {   //matching the input query
                if (st.nextToken().equals(word)) {

                    context.write(new Text(fileName), new Text(termFrequency));

                }

            }

        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, DoubleWritable> {
        @Override
        public void reduce(Text word, Iterable<Text> counts, Context context) //reducer function to display search results
                throws IOException, InterruptedException {
            double sum = 0.0;

            for (Text t : counts) {
                sum = sum + Double.parseDouble(t.toString());
            }

            context.write(new Text(word), new DoubleWritable(sum));

        }

    }
}