import java.io.IOException;

import java.util.StringTokenizer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public  class IndexMapper2
  extends Mapper<Object, Text, Text, Text> 
{
  private Text indKeys = new Text();
  private Text indValues = new Text();
  public void map( Object key, Text value, Context context )
      throws IOException, InterruptedException 
  {
        
        StringTokenizer strToken = new StringTokenizer(value.toString());
         while (strToken.hasMoreTokens()) 
         {
        	 indKeys.set(strToken.nextToken());
          if(strToken.hasMoreTokens())
          {
        	  indValues.set(strToken.nextToken());
          }
           context.write(indKeys, indValues);
         }

    }
}
