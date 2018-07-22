
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;


  public  class IndexReducer4
         extends Reducer<Text,Text,Text,Text> 
  {
    private Text txtWord = new Text();
    private String avgDocumentLength = Integer.toString(Variables.averagegDocumentLength);
    public void reduce(Text reduceKey, Iterable<Text> reduceValues,Context reduceContext) throws IOException, InterruptedException 
    {
    	
      for (Text txtVal : reduceValues) 
      {   	  
    	  txtWord.set(reduceKey.toString() + "," + avgDocumentLength );
    	  reduceContext.write(txtWord, txtVal);
      }
  }
}
