package bplus;

import java.util.Scanner;


public class TestBPTree {

	public static void main(String[] args) 
	{
		BPTree<Integer> tree = new BPTree<Integer>(4);
//		Scanner sc = new Scanner(System.in);
//		while(true) 
//		{
//			int x = sc.nextInt();
//			if(x == -1)
//				break;
//			tree.insert(x, null);
//			System.out.println(tree.toString());
//		}
//		while(true) 
//		{
//			int x = sc.nextInt();
//			if(x == -1)
//				break;
//			tree.delete(x);
//			System.out.println(tree.toString());
//		}
//		System.out.println(tree.search(2));
//		sc.close();
//		tree.delete(0,"O");
//		tree.delete(0,"O");
		tree.insert(1, new Ref("O",-1));
		tree.insert(1, new Ref("O",-1));
		tree.insert(0, new Ref("R",-1));
		tree.insert(2, new Ref("Z",-1));
		System.out.println(tree.search(2));
		//System.out.println(tree.search(1));
	}	
}
