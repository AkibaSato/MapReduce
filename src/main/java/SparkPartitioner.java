import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class SparkPartitioner extends Partitioner<Text, Text> {

	@Override
	public int getPartition(Text key, Text word, int i) {
		String wordLetter = key.toString();
		if (wordLetter.length() > 1) {
			int firstChar = wordLetter.charAt(0) - 'a';
			int secondChar = wordLetter.charAt(1) - 'a';	
			
			return (firstChar * 26 + secondChar) % i;
		}
		return 0;
	}


}
