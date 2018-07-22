
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;


  public  class IndexReducer3
         extends Reducer<Text,Text,Text,Text> 
  {
    private Text reducevValue = new Text();
    public void reduce(Text reduceKey, Iterable<Text> reduceValues,Context reduceContext) throws IOException, InterruptedException 
    {   
      String strValue = "<";
      double logWeight = 0;
      int numerOfDocumentss = Variables.numberofDocs;
      int doccumentLength = 0;
      for (Text txtVal : reduceValues) 
      {
    	  String[] termTmfreqDocfreq = txtVal.toString().split(",");
    	  logWeight += ( (1.0 + Math.log10(Integer.valueOf(termTmfreqDocfreq[1])) ) * Math.log10 ( numerOfDocumentss / Integer.valueOf(termTmfreqDocfreq[2]) )) *
                ( (1.0 + Math.log10(Integer.valueOf(termTmfreqDocfreq[1])) ) * Math.log10 ( numerOfDocumentss / Integer.valueOf(termTmfreqDocfreq[2]) )) ;
        
        doccumentLength += Integer.valueOf(termTmfreqDocfreq[1]);
        strValue += txtVal.toString() + ";";

      }
      Variables.totalDocumentLength += doccumentLength;
      logWeight = Math.sqrt(logWeight);
      strValue +=  ">";
      reduceKey.set(reduceKey.toString() + "," + Double.toString(logWeight) + "," + Integer.toString(doccumentLength));
      reducevValue.set(strValue);    
      reduceContext.write(reduceKey, reducevValue);
    }
  }
