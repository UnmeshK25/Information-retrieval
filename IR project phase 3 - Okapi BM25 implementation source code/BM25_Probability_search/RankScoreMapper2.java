import java.io.IOException;

import java.util.StringTokenizer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public  class RankScoreMapper2
  extends Mapper<Object, Text, Text, Text> 
{
	 private Text mapperKeys = new Text();
	  private Text mapperValues = new Text();
	  public void map( Object mapkey, Text mapvalue, Context context )
	      throws IOException, InterruptedException 
	  {
	        
	        StringTokenizer strToken = new StringTokenizer(mapvalue.toString());
	         while (strToken.hasMoreTokens()) 
	         {
	        	 mapperKeys.set(strToken.nextToken());
	        	 if(strToken.hasMoreTokens())
	        	 {
	        		 mapperValues.set(strToken.nextToken());
	             }
	           context.write(mapperKeys, mapperValues);
	         }

	    }
}
