
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;


public class Operator extends Configured implements Tool 
{

	private static final String FIRST_MAPREDUCE_OUTPUT_PATH = "./mapreduce1_Output";
	private static final String SECOND_MAPREDUCE_OUTPUT_PATH = "./termIndOP";
	private static final String THIRD_MAPREDUCE_OUTPUT_PATH = "./documentIndexOutput";
	private static final String FOURTH_MAPREDUCE_OUTPUT_PATH = "./documentIndexAverageLengthOutput";

	public int run(String[] args) throws Exception
	{		
		Configuration configure1 = new Configuration();
		Job MRjob = new Job(configure1);
		MRjob.setJobName("Hadoop job");
		MRjob.setJarByClass(Operator.class);

		MRjob.setMapperClass(IndexMapper.class);
		MRjob.setReducerClass(IndexReducer.class);

		MRjob.setInputFormatClass(ZipFileInputFormat.class);
		MRjob.setOutputFormatClass(TextOutputFormat.class);
		 
		MRjob.setOutputKeyClass(Text.class);
		MRjob.setOutputValueClass(Text.class);
		 
		ZipFileInputFormat.setInputPaths(MRjob, new Path(args[0]));
		TextOutputFormat.setOutputPath(MRjob, new Path(FIRST_MAPREDUCE_OUTPUT_PATH));

		MRjob.waitForCompletion(true);

		Configuration configure2 = new Configuration();
		Job MRjob2 = new Job(configure2);
		FileSystem filesys2 = FileSystem.get(configure2);
		filesys2.delete( new Path(FIRST_MAPREDUCE_OUTPUT_PATH + "/.*.crc"), true);
		MRjob2.setJobName("Hadoop job");

		MRjob2.setJarByClass(Operator.class);
		MRjob2.setMapperClass(IndexMapper2.class);
		MRjob2.setReducerClass(IndexReducer2.class);
		 
		MRjob2.setOutputKeyClass(Text.class);
		MRjob2.setOutputValueClass(Text.class);
		 
		TextInputFormat.addInputPath(MRjob2, new Path(FIRST_MAPREDUCE_OUTPUT_PATH + "/part-r-00000"));
		TextOutputFormat.setOutputPath(MRjob2, new Path(SECOND_MAPREDUCE_OUTPUT_PATH));
		

		String termAsKeyInd = "termAsKeyInd";
  		MultipleOutputs.addNamedOutput(MRjob2, termAsKeyInd, TextOutputFormat.class, Text.class, Text.class); 
  		MRjob2.waitForCompletion(true);

		Configuration configure3 = new Configuration();
		Job MRjob3 = new Job(configure3);
		FileSystem filesys3 = FileSystem.get(configure3);
		
		filesys3.delete( new Path(SECOND_MAPREDUCE_OUTPUT_PATH + "/.*.crc"), true);
		MRjob3.setJobName("Hadoop job");

		MRjob3.setJarByClass(Operator.class);
		MRjob3.setMapperClass(IndexMapper3.class);
		MRjob3.setReducerClass(IndexReducer3.class);
		 
		MRjob3.setOutputKeyClass(Text.class);
		MRjob3.setOutputValueClass(Text.class);

		TextInputFormat.addInputPath(MRjob3, new Path(SECOND_MAPREDUCE_OUTPUT_PATH + "/part-r-00000"));
		TextOutputFormat.setOutputPath(MRjob3, new Path(THIRD_MAPREDUCE_OUTPUT_PATH));
		MRjob3.waitForCompletion(true);

		Variables.averagegDocumentLength = Variables.totalDocumentLength / Variables.numberofDocs;
		
		Configuration configure4 = new Configuration();
		Job MRjob4 = new Job(configure4);
		FileSystem filesys4 = FileSystem.get(configure4);
		
		filesys4.delete( new Path(THIRD_MAPREDUCE_OUTPUT_PATH + "/.*.crc"), true);
		MRjob4.setJobName("Hadoop job");

		MRjob4.setJarByClass(Operator.class);
		MRjob4.setMapperClass(IndexMapper4.class);
		MRjob4.setReducerClass(IndexReducer4.class);
		 
		MRjob4.setOutputKeyClass(Text.class);
		MRjob4.setOutputValueClass(Text.class);
		 
		TextInputFormat.addInputPath(MRjob4, new Path(THIRD_MAPREDUCE_OUTPUT_PATH + "/part-r-00000"));
		TextOutputFormat.setOutputPath(MRjob4, new Path(FOURTH_MAPREDUCE_OUTPUT_PATH));

		return MRjob4.waitForCompletion(true) ? 0 : 1;
			
	}
	public static void main(String[] args) throws Exception 
	{
  		ToolRunner.run(new Configuration(), new Operator(), args);
	}
}
