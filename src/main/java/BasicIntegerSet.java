import java.util.*;

public class BasicIntegerSet {
	public int[] arr;
	public final int INTEGER_SIZE = 32;
	
	public BasicIntegerSet(int size){
		//Divide by 32 and round up, determines number of indices needed for our set
		this.arr = new int[(size + 31)  >> 5];
	}
	
	public boolean isSet(int x){
		//Determine array index (just integer / 32 rounded down)
		int i = (x > 0) ? x + 1 >> 5 : 0;
		if(i >= arr.length){
			return false;
		}		

		int val = arr[i];

		int res;
                if (i > 0) {
                        res = val | (0 | (1 << x % (i * INTEGER_SIZE)));
                } else {
                        res = val | (0 | (1 << x));
                }
		
		return res == val;
	}
	
	public void set(int x){
		int i = (x > 0) ? x + 1 >> 5 : 0;

		if (i >= arr.length) {
			resizeArr(i);
		}
		
		int val = arr[i];

		int res;
		if (i > 0) {
			res = val | (0 | (1 << x % (i * INTEGER_SIZE)));
		} else {
			res = val | (0 | (1 << x));
		}
		arr[i] = res;
	}

	private void resizeArr(int attemptedIndex){
		int newSize = (arr.length + attemptedIndex) * 2;
		int[] newArr = new int[newSize];
		
		for(int i = 0; i < arr.length; i++){
			newArr[i] = arr[i];
		}

		this.arr = newArr;
	}

	public String toString() {
		StringBuilder s = new StringBuilder();
		s.append("[");
		for(int i = 0; i < arr.length; i++){
			for(Integer position : bitPositions(arr[i])) {
				s.append((position-1 + i * INTEGER_SIZE) + ", ");
			}
		}
		s.setLength(s.length() - 2);
		s.append("]");

		return s.toString();
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
