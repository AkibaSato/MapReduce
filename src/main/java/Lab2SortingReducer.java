import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
//import java.util.List;
//import java.util.Set;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.json.JSONArray;
import org.json.JSONObject;

public class Lab2SortingReducer extends Reducer<Text, Text, Text, Text> {
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Iterator<Text> iter = values.iterator();
		BasicIntegerSet iS = new BasicIntegerSet(250);
		
		JSONArray ids = new JSONArray();
		JSONArray urls = new JSONArray();
		JSONArray title = new JSONArray();
		JSONArray snippet = new JSONArray();
		
		while(iter.hasNext()) {
		    String[] vals = iter.next().toString().split(",");
			int x = parseInt(vals[0]);
			x = Math.abs(x);
			if (!iS.isSet(x)) {
				ids.put(x);
				urls.put(vals[1]);
				title.put(vals[2]);
				snippet.put(vals[3]);
				iS.set(x);
			}
		}
		
		String jsonString = new JSONObject()
				.put("word", key.toString())
				.put("docID", ids)
				.put("title", title)
				.put("url", urls)
				.put("text", snippet)
				.toString();
		
		context.write(new Text(""),new Text(jsonString));
	}
	
	public static int parseInt( final String s ) {
	   	int num  = 0;
	    	final int len  = s.length( );
	    	num = '0' - s.charAt( 0 );

	    	// Build the number.
	    	int i = 1;
	    	while ( i < len )
			num = num*10 + '0' - s.charAt( i++ );
	    	return num;
	} 
}
