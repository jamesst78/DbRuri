package select_team_name;

import bplus.BRTree;
import bplus.Ref;

public class Testings {

	
	public static void main(String[] args) {
		
		BRTree<Double> testTree = new BRTree<Double>(2);
		
		Ref ref1 = new Ref("Page1" , 5);
		Ref ref2 = new Ref("Page2" , 5);
		Ref ref3 = new Ref("Page1" , 5);
		Ref ref4 = new Ref("Page2" , 5);
		
		testTree.insert(40.0, ref1);
		testTree.insert(11.0, ref2);
		testTree.insert(60.0, ref3);
		testTree.insert(11.0, ref4);
		
		System.out.println(testTree.toString()); 
		
		testTree.delete(40.0, "Page1");
		
		System.out.println(testTree.toString());
		
		
		
		
		
	}
}
