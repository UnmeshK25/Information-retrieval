import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;


  public  class IndexReducer2
         extends Reducer<Text,Text,Text,Text> 
  {
	  MultipleOutputs<Text, Text> multipleoutp;

      public void setup(Context context) 
      {
    	  multipleoutp = new MultipleOutputs<Text,Text>(context);
      } 

    private Text keyWord = new Text();
    private Text keyValue1 = new Text();

    public void reduce(Text key, Iterable<Text> values,Context context) 
    		throws IOException, InterruptedException 
    {
    	List<String> cacheList = new ArrayList<String>();
    	String strValue = "<";
    	for(Text txtVal : values) 
    	{
    		strValue += txtVal.toString().split(",")[0] + ";" ;
    		cacheList.add(txtVal.toString());
    	}
    	strValue += ">";

    	int documentfreq = cacheList.size();
      
      for(int i=0; i < cacheList.size(); i++) 
      {
    	  String[] docIDTermfreq = cacheList.get(i).split(",");
    	  keyWord.set(docIDTermfreq[0]);
    	  keyValue1.set(key.toString() + "," + docIDTermfreq[1] + "," + Integer.toString(documentfreq));
    	  context.write(keyWord,keyValue1);
      }

      String termAsKeyIndex = "termAsKeyIndex";
      key.set( key.toString() + "," + Integer.toString(documentfreq) );
      keyValue1.set(strValue);
      multipleoutp.write(termAsKeyIndex, key, keyValue1);
    }
    
    protected void clnup(Context context) throws IOException, InterruptedException {
    	multipleoutp.close();
    }
  }
