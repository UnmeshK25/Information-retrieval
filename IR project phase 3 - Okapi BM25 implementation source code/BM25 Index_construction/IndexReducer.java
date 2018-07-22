
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;


public  class IndexReducer
    extends Reducer<Text,Text,Text,Text> 
{
    private Text keyvalue1 = new Text();
    public void reduce(Text key, Iterable<Text> values,Context context) 
    		throws IOException, InterruptedException 
    {
      int termfreq = 0;
      for (Text textValue : values) 
      {
    	  termfreq +=  Integer.parseInt ( textValue.toString() );
      }
      String KeyStr1 = key.toString();
      String KeyStr2 = key.toString();

      key.set(KeyStr1.replaceAll("[0-9,]",""));
      keyvalue1.set(KeyStr2.replaceAll("[A-Za-z,]", "") + "," +  Integer.toString(termfreq));
      context.write(key, keyvalue1);
      
    }

  }
