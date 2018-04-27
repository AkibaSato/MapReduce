import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import opennlp.tools.stemmer.PorterStemmer;

public class Lab2DocMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	private Text docID = new Text();
	private Set<String> stopWords = this.scrubWords(); 
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		PorterStemmer stemmer = new PorterStemmer();
		String [] wordArray = value.toString().split(",");
		
		docID = new Text(wordArray[0]);
		String docContents = wordArray[3];
		String[] docArray = docContents.split("\\s+");
		
		for(int i = 0; i < docArray.length; i++) {
			String word = docArray[i].replaceAll("[^a-zA-Z ]", "").toLowerCase();
		    if (!stopWords.contains(word)){	
		    	String[] snippet;
		    	if(i + 10 < docArray.length) {
		    		snippet = Arrays.copyOfRange(docArray, i, i+10);
		    	} else {
		    		snippet = Arrays.copyOfRange(docArray, i, docArray.length);
		    	}
		    	context.write(new Text(stemmer.stem(word)), new Text(docID+","+wordArray[2]+","+i+","+String.join(" ", snippet)));
		    }	
		}
		 
	}
	
	private HashSet<String> scrubWords(){
		String[] stopWords = {"a", "about", "above", "after", "again", "against", "all", "am", "an", "and", "any", "are", "arent", 
				"as", "at", "be", "because", "been", "before", "being", "below", "between", "both", "but", "by", "cant", "cannot", 
				"could", "couldnt", "did", "didnt", "do", "does", "doesnt", "doing", "dont", "down", "during", "each", "few", 
				"for", "from", "further", "had", "hadnt", "has", "hasnt", "have", "havent", "having", "he", "hed", "hell", 
				"hes", "her", "here", "heres", "hers", "herself", "him", "himself", "his", "how", "hows", "i", "id", "ill", 
				"im", "ive", "if", "in", "into", "is", "isnt", "it", "its", "its", "itself", "lets", "me", "more", "most", 
				"mustnt", "my", "myself", "no", "nor", "not", "of", "off", "on", "once", "only", "or", "other", "ought", "our", 
				"ours", "ourselves", "out", "over", "own", "same", "shant", "she", "shed", "shell", "shes", "should", "shouldnt", 
				"so", "some", "such", "than", "that", "thats", "the", "their", "theirs", "them", "themselves", "then", "there", 
				"theres", "these", "they", "theyd", "theyll", "theyre", "theyve", "this", "those", "through", "to", "too", 
				"under", "until", "up", "very", "was", "wasnt", "we", "wed", "well", "were", "weve", "were", "werent", "what", 
				"whats", "when", "whens", "where", "wheres", "which", "while", "who", "whos", "whom", "why", "whys", "with", 
				"wont", "would", "wouldnt", "you", "youd", "youll", "youre", "youve", "your", "yours", "yourself"};
		return new HashSet<String>(Arrays.asList(stopWords));
	}
}
