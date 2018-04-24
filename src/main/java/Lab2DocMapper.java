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
		String URL = wordArray[1];
		String docContents = wordArray[3];
		String[] originalArray = docContents.split("\\s+");
		String[] docContentsArray = docContents.replaceAll("[^a-zA-Z ]", "").toLowerCase().split("\\s+");
		
		for(int i = 0; i < docContentsArray.length; i++) {
			String word = docContentsArray[i].toLowerCase();
		    if (!stopWords.contains(word)){
		    		String[] snippet;
		    		if(i + 10 > docContentsArray.length) {
		    			snippet = Arrays.copyOfRange(originalArray, i, docContentsArray.length);
		    		} else {
			    		snippet = Arrays.copyOfRange(originalArray, i, i+10);
		    		}
		    		
		    		context.write(new Text(stemmer.stem(word)), new Text(docID+","+URL+","+wordArray[2]+","+String.join(" ", snippet)));
		    }	
		}
		 
	}
	
	private HashSet<String> scrubWords(){
		String[] stopWords = {"a", "about", "above", "after", "again", "against", "all", "am", "an", "and", "any", "are", "aren't", 
				"as", "at", "be", "because", "been", "before", "being", "below", "between", "both", "but", "by", "can't", "cannot", 
				"could", "couldn't", "did", "didn't", "do", "does", "doesn't", "doing", "don't", "down", "during", "each", "few", 
				"for", "from", "further", "had", "hadn't", "has", "hasn't", "have", "haven't", "having", "he", "he'd", "he'll", 
				"he's", "her", "here", "here's", "hers", "herself", "him", "himself", "his", "how", "how's", "i", "i'd", "i'll", 
				"i'm", "i've", "if", "in", "into", "is", "isn't", "it", "it's", "its", "itself", "let's", "me", "more", "most", 
				"mustn't", "my", "myself", "no", "nor", "not", "of", "off", "on", "once", "only", "or", "other", "ought", "our", 
				"ours", "ourselves", "out", "over", "own", "same", "shan't", "she", "she'd", "she'll", "she's", "should", "shouldn't", 
				"so", "some", "such", "than", "that", "that's", "the", "their", "theirs", "them", "themselves", "then", "there", 
				"there's", "these", "they", "they'd", "they'll", "they're", "they've", "this", "those", "through", "to", "too", 
				"under", "until", "up", "very", "was", "wasn't", "we", "we'd", "we'll", "we're", "we've", "were", "weren't", "what", 
				"what's", "when", "when's", "where", "where's", "which", "while", "who", "who's", "whom", "why", "why's", "with", 
				"won't", "would", "wouldn't", "you", "you'd", "you'll", "you're", "you've", "your", "yours", "yourself"};
		return new HashSet<String>(Arrays.asList(stopWords));
	}
}
