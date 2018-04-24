/**
 * Efficient class for determining duplicate integers. Uses 1/4 the memory of a HashSet (since HashSet uses 
 * Integer wrapper, which is 16 bytes) and runs in O(1) without amortization. Used to remove duplicated docIds
 * in our Reducer function.
 * @author dsmyda
 */
import java.util.*;

public class IntegerSet {
	
	public CustomInteger[] arr;
	public final int INTEGER_SIZE = 32;
	
	public IntegerSet(int size){
		//Divide by 32 and round up, determines number of indices needed for our set
		this.arr = new CustomInteger[(size + 31)  >> 5];
	}
	
	public boolean isSet(int x){
		//Determine array index (just integer / 32 rounded down)
		int i = (x > 0) ? x + 1 >> 5 : 0;
		
		if (arr[i] != null) {
			int val = arr[i].i;

			int res;
			if(i > 0){
				res = val | (0 | (1 << (x % (i * INTEGER_SIZE))));
			} else {
				res = val | (0 | (1 << x));
			}
			return res == val;
		}
		
		return false;
	}

	public boolean isSet(int docId, int docPos) {
		int i = (docId > 0) ? docId + 1 >> 5 : 0;
		return isSet(docId) && arr[i].isSet(docId - i * INTEGER_SIZE + 1, docPos);
	}
	
	public void set(int docId, int docPos){
		int i = (docId > 0) ? docId + 1 >> 5 : 0;
		this.set(docId);

		if( Math.abs(i) > 0){
			arr[i].set(docId % (i * INTEGER_SIZE), docPos);
		} else {
			arr[i].set(docId, docPos);
		}
	}
	
	public void set(int docId){
		int i = (docId > 0) ? docId + 1 >> 5 : 0;

		if(i >= arr.length){
			resizeArr(i);	
		}

		if(arr[i] == null){
			arr[i] = new CustomInteger();
		}

		int val = arr[i].i;

		int res;
                if(i > 0){
                        res = val | (0 | (1 << (docId % (i * INTEGER_SIZE))));
                } else {
                        res = val | (0 | (1 << docId));
                }
		
		arr[i].i = res;
	}

	public void resizeArr(int attemptedIndex){
		int newSize = (arr.length + attemptedIndex)*2;
		CustomInteger[] newArr = new CustomInteger[newSize];

		for(int i = 0; i < arr.length; i++){
			newArr[i] = arr[i];
		}

		this.arr = newArr;
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();

		sb.append("[");
		for(int i = 0; i < arr.length; i++){
			if(arr[i] != null){
				for(Integer p : bitPositions(arr[i].i)){
					sb.append((p-1 + i * INTEGER_SIZE) + ", ");
				}
			}
		}
		sb.setLength(sb.length() - 2);
		sb.append("]");

		return sb.toString();
	}

        private static List<Integer> bitPositions(int number) {
                List<Integer> positions = new ArrayList<Integer>();
                int position = 1;
                while (number != 0) {
                        if ((number & 1) != 0) {
                                positions.add(position);
                        }
                        position++;
                        number = number >>> 1;
                }
                return positions;
        }
}
