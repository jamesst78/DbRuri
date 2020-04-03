package select_team_name;

import java.awt.Dimension;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Random;
import java.util.Vector;

import bplus.BPTree;
import bplus.Ref;



public class Page extends Vector {
	int N;
	String key;

	public Page(Tuple tuple, int N, String key) {
		this.N = N;
		this.key = key;
		this.add(tuple);
	}

	public Tuple insertIntoPage(Tuple t, boolean isLastPage) {
		// Last page has space
		if (isLastPage && this.size() < N) {
			this.add(t);
			this.sort(null);
			return null;
		}
		// Last page has no space tl3 mlhash lzma :(
//		else if(isLastPage&&this.size()==N) {
//			System.out.println("b" +t.theTuple.get("name"));
//			return t;
//		}
		// Element bigger than kol 7aga but not last page
		else if (t.compareTo(this.get(this.size() - 1)) > 0) {
			return t;

		}
		// Some element is larger than my element and page not full
		else if (this.size() < N && t.compareTo(this.get(this.size() - 1)) <= 0) {
			this.add(t);
			this.sort(null);
			return null;
		}
		// Some element is larger than my element and page is full even if last page
		else if (this.size() == N && t.compareTo(this.get(this.size() - 1)) <= 0) {
			this.add(t);
			this.sort(null);
			t = (Tuple) this.remove(this.size() - 1);
			return t;
		}
		else {
			System.out.println("wtf");
		}

		return null;
	}

	public void deleteFromPage(Hashtable<String, Comparable> ht) {
		ArrayList<String> columnNames = new ArrayList<String>();
		ArrayList<Comparable> columnValues = new ArrayList<Comparable>();

		Enumeration<String> enumeration = ht.keys();
		// iterate using enumeration object
		while (enumeration.hasMoreElements()) {

			String key = enumeration.nextElement();
			columnNames.add(key);
			columnValues.add((Comparable) ht.get(key));
		}
		for (int i = 0; i < this.size(); i++) {
			Tuple t = (Tuple) this.get(i);
			boolean satisfiesCondition = true;

			for (int j = 0; j < columnNames.size(); j++) {
				if (!(t.theTuple.get(columnNames.get(j)).compareTo(columnValues.get(j))==0)) {
					System.out.println((t.theTuple.get(columnNames.get(j)).compareTo(columnValues.get(j))));
					satisfiesCondition = false;
				}
			}
			if (satisfiesCondition)
				this.remove(i);
		}
	}

	public void updatePage(String strTableName, Comparable key, Hashtable<String, Comparable> ht)
			throws IOException, ClassNotFoundException {
		int i = getFirstOccurence(key);
		if (i == -1) {
			return;
		}
		for (int j = i; j < this.size(); j++) {
			Tuple t = (Tuple) this.get(j);
			if (!t.theTuple.get(this.key).equals(key)) {
				return;
			}
			Enumeration<String> enumeration = ht.keys();
			// iterate using enumeration object
			while (enumeration.hasMoreElements()) {

				String myKey = enumeration.nextElement();
				t.theTuple.put(myKey, ht.get(myKey));

			}

		}
	}

	public int getFirstOccurence(Comparable key) {
		int counter = 1;
		int first = 0;
		int last = size() - 1;

		int mid = (first + last) / 2;
		while (first <= last) {
			System.out.println("Iteration "+counter);
			System.out.println("Pointer met "+(first==last));
			
			if (((Tuple)this.get(mid)).theTuple.get(this.key).equals(key) && (mid==0||(((Tuple) this.get(mid-1)).theTuple.get(this.key).compareTo(key) < 0) )) {
				System.out.println("Else if");
				return mid;
			
			} else if (((Tuple) this.get(mid)).theTuple.get(this.key).compareTo(key) < 0) {
				System.out.println("First if");
				first = mid + 1; 
			} else {
				System.out.println("Else");
				last = mid - 1;
			}
			mid = (first + last) / 2;
			counter++;
		}
		System.out.println(first);
		System.out.println(last);
		if (first > last) {
			System.out.println("Might be Edge case");
			if (last + 1 < this.size() && ((Tuple)this.get(last + 1)).theTuple.get(this.key).equals(key)) {
				System.out.println("Actually Edge case");
				return last + 1;
			}
		}
		return -1;
	}
	
	
	
	public BPTree<Double> fillRTree(BPTree<Double> tree , String strColName , String pageName) {
			
		for(int i = 0 ; i<this.size() ; i++) {
			
			Tuple t = (Tuple) this.get(i);
		
			Polygon p = (Polygon) t.theTuple.get(strColName);
			Dimension dim = p.getBounds( ).getSize();
			double ThisArea = dim.width * dim.height;
			
			Ref r = new Ref(pageName, 6);
			
			tree.insert(ThisArea, r);
					
		}
		//tree.visualize();
		System.out.println("got to return");
		return tree;	
		
	}
	
	
	
	

}
