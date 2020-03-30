package select_team_name;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;

public class Tuple implements Comparable, Serializable{
	Hashtable<String, Comparable> theTuple;
	String keyS;
	TupleIdentification identification; 
	public Tuple(Hashtable<String, Comparable> theTuple, String keyS) {
		this.theTuple = theTuple;
		this.keyS = keyS;
	}
	@Override
	public int compareTo(Object o) {
		Tuple t = (Tuple) o;
		return this.theTuple.get(keyS).compareTo(t.theTuple.get(keyS));
	}
	
	public static void main(String[]args) {
		System.out.println(new Integer(4).compareTo(new Integer(3)));
	}
	
	public String toString(){
		String str = "";
		Enumeration<String> en = theTuple.keys();
		while(en.hasMoreElements()) {
			String k = en.nextElement();
			str+= k + " "+ theTuple.get(k)+"--";
		}
		return str+"\n";
	}
}
