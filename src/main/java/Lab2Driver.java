import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Lab2Driver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Path inp1 = new Path(args[0]);
		//Path inp2 = new Path(args[1]);
		//Path inp3 = new Path(args[2]);

		Path out = new Path(args[1]);
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "inverted index");
		
		job.setNumReduceTasks(676);

		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(job, inp1);
		//TextInputFormat.addInputPath(job, inp2);
		//TextInputFormat.addInputPath(job, inp3);

		
		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, out);
		
		job.setJarByClass(Lab2Driver.class);
		job.setMapperClass(Lab2DocMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setReducerClass(Lab2SortingReducer.class);
		
        job.setPartitionerClass(SparkPartitioner.class);
//        job.setGroupingComparatorClass(WikiComparator.class);
		
		job.waitForCompletion(true);
	}
}