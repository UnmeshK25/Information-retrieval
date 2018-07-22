import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.util.Tool;
import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class Operator extends Configured implements Tool 
{

	private HashMap<String,Integer>  stoppedWordMap = Variables.stopWordMap;
	private HashMap<String,Integer>  queryTermDocfreqMap = Variables.queryTermDocumentfreqMap;
 	private int numberOfDocuments = Variables.numberOfDocs;

	private String First_Inp = "/user/hduser/input10/termAsKeyIndex-r-00000";
	private String Relevant_Document_Out = "./relevantDocumentsOutput";

	private String Second_Inp = "/user/hduser/input10/part-r-00000";
	private String Relevant_document_score_op = "./relevantDocumentScoreOutput";

	private String Relevant_document_score_as_inp = "relevantDocScoreOutput/part-r-00000";
	private String top_Rank_Document_Output = "./topRankDocumentOutput";

	private String Content_op_as_inp = "/user/hduser/input10/contentOP";
	private String rank_result_content = "./contentOutput";

 	public static double moldScoreOfQuery = 0.0;

	public int run(String[] args) throws Exception
	{

        Configuration configure = new Configuration();

		Job mapReduce_job = new Job(configure);
		FileSystem fileSys = FileSystem.get(configure);

		fileSys.delete( new Path(First_Inp + "./.*.crc"), true);
	
		mapReduce_job.setJobName("Hadoop job");
		mapReduce_job.setJarByClass(Operator.class);
		mapReduce_job.setMapperClass(RankScoreMapper.class);
		mapReduce_job.setReducerClass(RankScoreReducer.class);

		mapReduce_job.setOutputKeyClass(Text.class);
		mapReduce_job.setOutputValueClass(Text.class);

		TextInputFormat.addInputPath(mapReduce_job, new Path(First_Inp));
		TextOutputFormat.setOutputPath(mapReduce_job, new Path(Relevant_Document_Out));
		
		mapReduce_job.waitForCompletion(true);

		Iterator iter = stoppedWordMap.entrySet().iterator();
		while (iter.hasNext()) 
		{
            Map.Entry map_reducePair = (Map.Entry)iter.next();
            String keyStr = (String)map_reducePair.getKey();
            Integer value = (Integer)map_reducePair.getValue();
            try{
            	moldScoreOfQuery += (1.0 + Math.log10(value)) *(Math.log10(numberOfDocuments / (Integer)queryTermDocfreqMap.get(keyStr)) )*
                        (1.0 + Math.log10(value)) *(Math.log10(numberOfDocuments / (Integer)queryTermDocfreqMap.get(keyStr)) );
            }catch(NullPointerException e)
            {
           
            }
        }
		moldScoreOfQuery = Math.sqrt(moldScoreOfQuery);

		Configuration configure2 = new Configuration();
		Job mapReduce_job2 = new Job(configure2);
		FileSystem fileSys2 = FileSystem.get(configure2);
		fileSys2.delete( new Path(Second_Inp + "/.*.crc"), true);
		mapReduce_job2.setJobName("Hadoop job");

		mapReduce_job2.setJarByClass(Operator.class);
		mapReduce_job2.setMapperClass(RankScoreMapper2.class);
		mapReduce_job2.setReducerClass(RankScoreReducer2.class);
		 
		mapReduce_job2.setOutputKeyClass(Text.class);
		mapReduce_job2.setOutputValueClass(Text.class);
		 
		TextInputFormat.addInputPath(mapReduce_job2, new Path(Second_Inp));
		TextOutputFormat.setOutputPath(mapReduce_job2, new Path(Relevant_document_score_op));

		mapReduce_job2.waitForCompletion(true);

		Configuration configure3 = new Configuration();
		Job mapReduce_job3 = new Job(configure3);
		FileSystem fileSys3 = FileSystem.get(configure3);
		fileSys3.delete( new Path(Relevant_document_score_as_inp + "/.*.crc"), true);
		mapReduce_job3.setJobName("Hadoop job");

		mapReduce_job3.setJarByClass(Operator.class);
		mapReduce_job3.setMapperClass(RankScoreMapper3.class);
		mapReduce_job3.setReducerClass(RankScoreReducer3.class);
		 
		mapReduce_job3.setOutputKeyClass(NullWritable.class);
		mapReduce_job3.setOutputValueClass(Text.class);
		 
		TextInputFormat.addInputPath(mapReduce_job3, new Path(Relevant_document_score_as_inp));
		TextOutputFormat.setOutputPath(mapReduce_job3, new Path(top_Rank_Document_Output));

		mapReduce_job3.waitForCompletion(true);

		Configuration configure4 = new Configuration();
		Job mapReduce_job4 = new Job(configure4);
		FileSystem fs4 = FileSystem.get(configure4);
		fs4.delete( new Path(Content_op_as_inp + "/.*.crc"), true);
		mapReduce_job4.setJobName("Hadoop job");

		mapReduce_job4.setJarByClass(Operator.class);
		mapReduce_job4.setMapperClass(RankScoreMapper4.class);
		mapReduce_job4.setReducerClass(RankScoreReducer4.class);
		 
		mapReduce_job4.setOutputKeyClass(Text.class);
		mapReduce_job4.setOutputValueClass(Text.class);
		 
		TextInputFormat.addInputPath(mapReduce_job4, new Path(Content_op_as_inp));
		TextOutputFormat.setOutputPath(mapReduce_job4, new Path(rank_result_content));

		return mapReduce_job4.waitForCompletion(true) ? 0:1;

	}

	public static void main(String[] args) throws Exception 
	{
  		ToolRunner.run(new Configuration(), new Operator(), args);
	}
}
