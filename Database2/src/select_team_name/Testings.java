package select_team_name;

import bplus.BRTree;
import bplus.Ref;

public class Testings {

	
	public static void main(String[] args) {
		
//		BRTree<Double> testTree = new BRTree<Double>(2);
//		
//		Ref ref1 = new Ref("Page1" , 5);
//		Ref ref2 = new Ref("Page2" , 5);
//		Ref ref3 = new Ref("Page1" , 5);
//		Ref ref4 = new Ref("Page2" , 5);
//		
//		testTree.insert(40.0, ref1);
//		testTree.insert(11.0, ref2);
//		testTree.insert(60.0, ref3);
//		testTree.insert(11.0, ref4);
//		
//		System.out.println(testTree.toString()); 
//		
//		testTree.delete(40.0, "Page1");
//		
//		System.out.println(testTree.toString());
		
		int [] x = {0,2,0,2};
		int [] y = {0,0,2,2};
		int [] z = {7,7,7,7};
		
		int [] x2 = {2,4,2,4};
		int [] y2 = {0,0,2,2};
		int [] z2 = {7,7,7,7};
		
		int [] x3 = {4,2,4,2};
		int [] y3 = {2,2,0,0};
		
		
		Polygon p = new Polygon(x,y,4);
		Polygon p2 = new Polygon(x2, y2, 4);
		Polygon p3 = new Polygon(x3, y3, 4);
		
		System.out.println(p3.uniqueEqual(p));
		
		
		
		
		
	}
}
