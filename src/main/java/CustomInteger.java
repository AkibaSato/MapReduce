public class CustomInteger {
	int i = 0;
	BasicIntegerSet[] a = new BasicIntegerSet[32];
	
	public void set(int pos, int x){
		if (a[pos] == null){
			a[pos] = new BasicIntegerSet(250);
		}
		a[pos].set(x);
	}

	public boolean isSet(int pos, int x) {
		if(a[pos] == null) {
			return false;
		}
		return a[pos].isSet(x);
	}
}
