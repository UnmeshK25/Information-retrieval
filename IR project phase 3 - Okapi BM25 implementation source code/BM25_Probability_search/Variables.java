import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.io.IOException;
import java.io.FileNotFoundException;

public final class Variables 
{
    public final static int numberOfDocs = 808311;

    public static HashMap<String, Integer> relvantDocumentsMap;
    static {
    	relvantDocumentsMap = new HashMap<String,Integer>();
    }
    public static HashMap<String, Double> documentScoreMap;
    static {
    	documentScoreMap = new HashMap<String,Double>();

    }

    public static HashMap<String, Integer> queryTermDocumentfreqMap;
    static {
    	queryTermDocumentfreqMap = new HashMap<String,Integer>();
    }

    public static HashMap<String, Double> rankedDocumentResultMap;

    static {
    	rankedDocumentResultMap = new HashMap<String,Double>();
    
    }
    public static final HashMap<String, Integer> stopWordMap;
    static 
    {
    	stopWordMap = new HashMap<String,Integer>();
        try
        {   
            BufferedReader buffReadInput = new BufferedReader(new FileReader("/home/ukancha/term"));
            String queryTerm = "";
            queryTerm = buffReadInput.readLine();
            buffReadInput.close();

            queryTerm = queryTerm.toLowerCase();
            queryTerm = Stopwords.stopWordsRemover(queryTerm);
     
            String[] totalTermsinQ = queryTerm.split("\\s+");
     
            for(int i = 0; i < totalTermsinQ.length; i++)
            {
          
                String stopWords = totalTermsinQ[i];
                Integer value = stopWordMap.get(stopWords);
                if (value != null) 
                {
                	stopWordMap.put(stopWords, stopWordMap.get(stopWords) + 1);
                }
                else
                {
                	stopWordMap.put(stopWords,1);
                }
            }
        }catch(FileNotFoundException e)
        {
    
            e.printStackTrace();
        }
        catch(IOException e)
        {
   
            e.printStackTrace();
        }
    }

   



}
