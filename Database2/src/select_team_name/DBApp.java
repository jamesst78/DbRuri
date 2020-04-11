package select_team_name;



import java.util.List;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.lang.reflect.Array;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.ThreadLocalRandom;

import bplus.BPTree;
import bplus.BRTree;



public class DBApp {

	public DBApp() throws IOException {
		String dir = "data\\metadata.csv";
		File file = new File(dir);
		if (file.createNewFile()) {
			FileWriter myWriter = new FileWriter(dir, true);
			myWriter.write("Table Name, Column Name, Column Type, ClusteringKey, Indexed\n");
			myWriter.close();
		}
	}

	public void createTable(String tableName, String key, Hashtable ht) throws DBAppException, IOException {
		String row = "";
		BufferedReader csvReader = new BufferedReader(new FileReader("data\\metadata.csv"));
		while ((row = csvReader.readLine()) != null) {
			String[] data = row.split(",");
			// do something with the data
			if (data[0].equals(tableName)) {
				
				throw new DBAppException("Table already exists");
			}
		}
		if(ht.isEmpty())
			throw new DBAppException("Table must have columns");
		if(!ht.containsKey(key)) {
			throw new DBAppException("Clustering key must be a column in the table");
		}
		new Table(tableName, key, ht);
		csvReader.close();
	}

	public void insertIntoTable(String tableName, Hashtable<String, Object> ht)
			throws IOException, ClassNotFoundException, DBAppException {
		String dir = "data\\" + tableName + ".txt";

		Hashtable<String, Comparable> newHt = new Hashtable<String, Comparable>();
		Enumeration<String> enumeration = ht.keys();
		// iterate using enumeration object
		String row = "";
		String type = "";
		String clusterkey = "";
		ArrayList inputKeys = new ArrayList<>();
		ArrayList inputkeysValues = new ArrayList<>();
		ArrayList inputKeysValuesClass = new ArrayList<>();
		ArrayList<String> columnsName = new ArrayList<>();
		ArrayList columnsType = new ArrayList<>();
		while (enumeration.hasMoreElements()) {
			// get the values and keys of the input hashtable
			String htKey = enumeration.nextElement();
			inputKeys.add(htKey);
			inputkeysValues.add(ht.get(htKey));
			inputKeysValuesClass.add(ht.get(htKey).getClass() + "");
			newHt.put(htKey, (Comparable) ht.get(htKey));
		}

		BufferedReader csvReader = new BufferedReader(new FileReader("data\\metadata.csv"));
		// get the needed values from the csv file
		while ((row = csvReader.readLine()) != null) {
			String[] data = row.split(",");
			if (data[0].equals(tableName)) {
				columnsName.add(data[1]);
				if(!ht.containsKey(data[1])&&!data[1].equals("TouchDate"))
					throw new DBAppException("Must insert value for column "+ data[1]);
				columnsType.add(data[2]);
				if (data[3].equals("true")) {
					clusterkey = data[1];
				}
			}
		}
		csvReader.close();
		
		// System.out.println(clusterkey);
		if (clusterkey == "") {
			throw new FileNotFoundException("File / table not found exception");
		}
		// check the cluster
		if (!inputKeys.contains(clusterkey)) {
			System.out.println(inputKeys.toString());
			throw new DBAppException("Cluster key isnt inserted ");
		}
		// check correct columns
		for (int i = 0; i < inputKeys.size(); i++) {
			if (!columnsName.contains(inputKeys.get(i))) {
				throw new DBAppException("Wrong column name");
			}

		}

		// System.out.println(columnsName);
		// System.out.println(columnsType);

		// check the types correct
		int index = 0;
		String csvType = "";
		for (int i = 0; i < inputKeys.size(); i++) {
			for (int j = 0; j < columnsName.size(); j++) {
				if (inputKeys.get(i).equals(columnsName.get(j))) {
					index = j;
					break;
				}
			}
			// got the index of the columnname , get the coulmn type
			csvType = (String) columnsType.get(index);

			switch (csvType) {

			case ("java.lang.Integer"):
				if (!inputKeysValuesClass.get(i).equals("class java.lang.Integer")) {
					throw new DBAppException("Expected Integer, type mismatch");
				}
				break;
			case ("java.lang.Double"):
				if (!inputKeysValuesClass.get(i).equals("class java.lang.Double")) {
					throw new DBAppException("Expected Double, type mismatch");
				}
				break;
			case ("java.lang.Boolean"):
				if (!inputKeysValuesClass.get(i).equals("class java.lang.Boolean")) {
					throw new DBAppException("Expected Boolean, type mismatch");
				}
				break;
			case ("java.awt.Polygon"):
				newHt.put(columnsName.get(index), Polygon.parsePolygon(inputkeysValues.get(i).toString()));
				break;
			case "java.util.Date":
				SimpleDateFormat formatter = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
				try {
					newHt.put(columnsName.get(index), formatter.parse(inputkeysValues.get(i).toString()));
				} catch (ParseException e) {
					throw new DBAppException("Invalid date format entered");
				}
				break;
			case ("java.lang.String"):
				if (!inputKeysValuesClass.get(i).equals("class java.lang.String")) {
					throw new DBAppException("Expected String, type mismatch");
				}

			}

		}

		// deserialize
		FileInputStream file = new FileInputStream(dir);
		ObjectInputStream in = new ObjectInputStream(file);

		// Method for deserialization of object
		Table t = (Table) in.readObject();
		in.close();
		file.close();
		t.insertIntoTable(tableName, newHt);
		// serialize
		FileOutputStream fileO = new FileOutputStream(dir);
		ObjectOutputStream out = new ObjectOutputStream(fileO);
		out.writeObject(t);

		out.close();
		fileO.close();
		
		csvReader.close();
	}

	public void deleteFromTable(String strTableName, Hashtable<String, Object> ht)
			throws ClassNotFoundException, IOException, DBAppException {
		String row = "";
		ArrayList<String> rtreeCol = new ArrayList<String>();
		ArrayList<String> btreeCol = new ArrayList<String>();
		BufferedReader csvReader = new BufferedReader(new FileReader("data\\metadata.csv"));
		boolean flag = false;
		while ((row = csvReader.readLine()) != null) {
			String[] data = row.split(",");
			// do something with the data
			if (data[0].equals(strTableName)) {
				flag = true;
			}
			if(data[0].equals(strTableName) && data[4].equals("true") && ht.containsKey(data[1])) {
				//we should check the type
				if(data[2].equals("java.awt.Polygon")) {
				rtreeCol.add(data[1]);
				}
				else {
					btreeCol.add(data[1]);
				}
			}
		}
		if(!flag)
			throw new DBAppException("Table does not exsist");
		Hashtable<String, Comparable> newHt = new Hashtable<String, Comparable>();
		Enumeration<String> enumeration = ht.keys();
		// iterate using enumeration object
		while (enumeration.hasMoreElements()) {

			String htKey = enumeration.nextElement();
			newHt.put(htKey, (Comparable) ht.get(htKey));
		}
		// deserialize
		FileInputStream file = new FileInputStream("data\\" + strTableName + ".txt");
		ObjectInputStream in = new ObjectInputStream(file);

		// Method for deserialization of object
		Table t = (Table) in.readObject();
		
		if(rtreeCol.isEmpty() && btreeCol.isEmpty()) {
			t.deleteFromTable(strTableName, newHt);
		}
		else {
			if(!rtreeCol.isEmpty()) {
			ArrayList<BRTree<Double>> bRAll  = new ArrayList<BRTree<Double>>();
		  for(int i = 0;i<rtreeCol.size();i++){				//Im now deserializing all the RTrees I found made for the columns in input ht
			String tPath = "data\\"+strTableName+"_"+rtreeCol.get(i) + ".txt";
			FileInputStream fTree = new FileInputStream(tPath);
			ObjectInputStream inTree = new ObjectInputStream(fTree);
			BRTree<Double> bP = (BRTree<Double>)inTree.readObject();    
			bRAll.add(bP);												//adding those RTrees to an array to pass it to table delete using RTree
			fTree.close();
			inTree.close();	
			}		  
			t.deleteFromTableBTreeR(strTableName,newHt,bRAll,rtreeCol);
			}
			
			if(!btreeCol.isEmpty()) {
				 
					ArrayList<BPTree<String>> bPAll  = new ArrayList<BPTree<String>>();
				  for(int i = 0;i<rtreeCol.size();i++){				//Im now deserializing all the RTrees I found made for the columns in input ht
					String tPath = "data\\"+strTableName+"_"+btreeCol.get(i) + ".txt";
					FileInputStream fTree = new FileInputStream(tPath);
					ObjectInputStream inTree = new ObjectInputStream(fTree);
					BPTree<String> bP = (BPTree<String>)inTree.readObject();    
					bPAll.add(bP);												//adding those RTrees to an array to pass it to table delete using RTree
					fTree.close();
					inTree.close();	
					}		  
					t.deleteFromTableBTreeB(strTableName,newHt,bPAll,btreeCol);
					
			}
// serialize the trees after deleting			
//			for (int x = 0; x < bPAll.size(); x++) {
//				FileOutputStream fo = new FileOutputStream(
//						new File("data/" + strTableName + "_" + btreeCol.get(x) + ".txt"));
//				ObjectOutputStream oj = new ObjectOutputStream(fo);
//				oj.writeObject(bPAll.get(x));
//				fo.close();
//				oj.close();
//			}
		}
		
		
		ArrayList<String> toRem = new ArrayList<String>();
		for (String pageDir : t.pages) {
			FileInputStream fileP = new FileInputStream(pageDir);
			ObjectInputStream inP = new ObjectInputStream(fileP);

			Page p = (Page) inP.readObject();

			inP.close();
			fileP.close();
			if (p.size() == 0) {
				Path path = Paths.get(pageDir);
				Files.delete(path);
				toRem.add(pageDir);
			}
		}
		t.pages.removeAll(toRem);
		in.close();
		file.close();

		// serialize
		FileOutputStream fileO = new FileOutputStream("data\\" + strTableName + ".txt");
		ObjectOutputStream out = new ObjectOutputStream(fileO);
		out.writeObject(t);

		out.close();
		fileO.close();

	}

	public void updateTable(String tableName, String key, Hashtable<String, Object> ht)
			throws IOException, ClassNotFoundException, DBAppException {
		String row = "";
		BufferedReader csvReader = new BufferedReader(new FileReader("data\\metadata.csv"));
		boolean flag = false;
		while ((row = csvReader.readLine()) != null) {
			String[] data = row.split(",");
			// do something with the data
			if (data[0].equals(tableName)) {
				flag = true;
			}
		}
		if(!flag)
			throw new DBAppException("Table does not exist");
		Hashtable<String, Comparable> newHt = new Hashtable<String, Comparable>();
		Enumeration<String> enumeration = ht.keys();
		// iterate using enumeration object
		while (enumeration.hasMoreElements()) {

			String htKey = enumeration.nextElement();
			newHt.put(htKey, (Comparable) ht.get(htKey));
		}
		String dir = "data\\" + tableName + ".txt";
		// deserialize
		FileInputStream file;
		try {
			file = new FileInputStream(dir);
		} catch (FileNotFoundException e) {
			throw new DBAppException("Table " + tableName + " not found");
		}
		ObjectInputStream in = new ObjectInputStream(file);

		// Method for deserialization of object
		Table t = (Table) in.readObject();
		try {
			t.updateTable(tableName, key, newHt);
		} catch (DBAppException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		in.close();
		file.close();

		// serialize
		FileOutputStream fileO = new FileOutputStream(dir);
		ObjectOutputStream out = new ObjectOutputStream(fileO);
		out.writeObject(t);

		out.close();
		fileO.close();
	}
	
	
	// Muhad && Farah && yuka R tree
	
public void createRTreeIndex(String strTableName , String strColName) throws DBAppException, IOException, ClassNotFoundException
	{
		//TODO: change to 255 later
		BRTree tree = new BRTree<Double>(2);  //2 for testing , later 255
		
		//han assume el Column type is a polygon f3lan
		
		//we shall go deserialize el table
		String pageString = "";
		String dir = "data\\" + strTableName + ".txt";
		String treeDir = "data\\" + strTableName +"_" + strColName + ".txt";
		FileInputStream file = new FileInputStream(dir);
		ObjectInputStream in = new ObjectInputStream(file);
		
		Table t = (Table) in.readObject();
		
		for(int i = 0 ; i<t.pages.size() ; i++) {
			pageString = t.pages.get(i);
			FileInputStream file2 = new FileInputStream(pageString);
			ObjectInputStream in2 = new ObjectInputStream(file2);
			//do something with the page
			System.out.println("PageString : " + pageString);
			Page p = (Page) in2.readObject();
			tree =  p.fillRTree(tree , strColName, t.pages.get(i));
			//
		
			in2.close();
			file2.close();
			
			//serialize the page back
			FileOutputStream fileO = new FileOutputStream(pageString);
			ObjectOutputStream out = new ObjectOutputStream(fileO);
			out.writeObject(p);
			

			out.close();
			fileO.close();
		}
		
		in.close();
		file.close();
		
		//TODO : dont forget to serialize the table after finishing
		FileOutputStream file3 = new FileOutputStream(dir);
		ObjectOutputStream out3 = new ObjectOutputStream(file3);
		out3.writeObject(t);

		out3.close();
		file3.close();
		
			
		//--------------------------------------------------------------------------
		
		
		System.out.println(tree.toString());
		
		//TODO : Serialize the tree into a file
		
		
		
		FileOutputStream file4 = new FileOutputStream(treeDir);
		ObjectOutputStream out4 = new ObjectOutputStream(file4);
		out4.writeObject(tree);
		out4.close();
		file4.close();
		
		File csfile = new File("data/temp.csv");
		try {
			FileWriter fw = new FileWriter(csfile, true);
			BufferedWriter bw = new BufferedWriter(fw);
			PrintWriter pw = new PrintWriter(bw);
			File old = new File("data/metadata.csv");
			Scanner x = new Scanner(old);
			x.useDelimiter("[,\n]");
			
			while(x.hasNext()) {
				String tableName = x.next();
				String columnName = x.next();
				String type = x.next();
				String isKey =x.next();
				String isIndexed = x.next();
				
				if(columnName.equals(strColName)) {
					pw.print(tableName+","+columnName+","+type+","+isKey+","+"true");
				}
				else {
					pw.print(tableName+","+columnName+","+type+","+isKey+","+isIndexed);
				}
				pw.print("\n");
			}
			x.close();
			pw.flush();
			fw.close();
			bw.close();
			pw.close();
			Files.delete(Paths.get("data/metadata.csv"));
			File dump = new File("data/metadata.csv");
			csfile.renameTo(dump);
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		
		
		
		
	}	
	
	
	
	public static void main(String[] args) throws ClassNotFoundException, IOException, DBAppException {
		// eiad is not here t3alo n7ot both marra w7da
		DBApp db = new DBApp();
		Hashtable<String, String> ht = new Hashtable<String, String>();
		ht.put("id", "java.lang.Integer");
		ht.put("name", "java.lang.String");
	//	ht.put("gpa", "java.lang.Double");
		//ht.put("isHappy", "java.lang.Boolean");
	//	ht.put("birthDay", "java.util.Date");
		ht.put("home", "java.awt.Polygon");
		ht.put("office", "java.awt.Polygon");

		Hashtable<String, Object> ht1 = new Hashtable<String, Object>();
		ht1.put("id", new Integer(1));
		ht1.put("name", "Eiad");
	//	ht1.put("gpa", 0.7);
	//	ht1.put("isHappy", false);
		//ht1.put("birthDay", "04/08/1999 03:30:04");
		ht1.put("home", "(0,0),(1,1),(2,2)");
		ht1.put("office", "(0,0),(5,5),(7,7)");
		

		Hashtable<String, Object> ht2 = new Hashtable<String, Object>();
		ht2.put("id", new Integer(2));
		//ht2.put("gpa", 1.5);
		ht2.put("name", "Mohab2");
		ht2.put("home", "(0,0),(7,3),(2,2)");
		ht2.put("office", "(0,0),(5,5),(7,7)");


		Hashtable<String, Object> ht3 = new Hashtable<String, Object>();
		ht3.put("id", new Integer(3));
		ht3.put("name", "Farahat4");
		ht3.put("home", "(0,0),(45,52),(32,25)");
		ht3.put("office", "(0,0),(9,9),(4,1)");
		
		Hashtable<String, Object> ht4 = new Hashtable<String, Object>();
		ht4.put("id", new Integer(4));
		ht4.put("name", "Mai4");
		ht4.put("home", "(10,60),(11,55),(14,22)");
		ht4.put("office", "(0,0),(9,9),(4,1)");
		
		Hashtable<String, Object> ht5 = new Hashtable<String, Object>();
		ht5.put("id", new Integer(5));
		ht5.put("name", "Ahmed5");
		ht5.put("home", "(30,30),(80,40),(90,50)");
		ht5.put("office", "(0,0),(4,4),(4,8)");
		
		Hashtable<String, Object> ht6 = new Hashtable<String, Object>();
		ht6.put("id", new Integer(6));
		ht6.put("name", "Zeiyad6");
		ht6.put("home", "(21,23),(51,57),(22,92)");
		ht6.put("office", "(0,6),(2,3),(8,9)");
		
		Hashtable<String, Object> ht7 = new Hashtable<String, Object>();
		ht7.put("id", new Integer(7));
		ht7.put("name", "Salma7");
		ht7.put("home", "(1,1),(0,4),(2,7)");
		ht7.put("office", "(0,5),(0,0),(0,9)");
		
		Hashtable<String, Object> ht8 = new Hashtable<String, Object>();
		
		ht8.put("name", "Salma7");
		ht8.put("office", "(0,5),(0,0),(0,9)");
		
		
		
		

		db.createTable("People", "id", ht);
		db.insertIntoTable("People", ht1);
		db.insertIntoTable("People", ht2);
		db.insertIntoTable("People", ht3);
		db.insertIntoTable("People", ht4);
		db.insertIntoTable("People", ht5);
		db.insertIntoTable("People", ht6);
		db.insertIntoTable("People", ht7);
		
		System.out.println(  "Salma's office is : "+((Polygon)Polygon.parsePolygon(ht8.get("office")+ "")).getArea());
		
		db.createRTreeIndex("People", "home");
		db.createRTreeIndex("People", "office");
		
		
		//Print the trees , Print the table before and after
		

//		
//		System.out.println("PRINTING TABLEEEEEEE");
//		String dir = "data\\People.txt";
//		FileInputStream f = new FileInputStream(dir);
//		ObjectInputStream in = new ObjectInputStream(f);
//
//		Table t = (Table) in.readObject();
//		
//		String dir1 = t.pages.get(0);
//
//		FileInputStream f1 = new FileInputStream(dir1);
//		ObjectInputStream in1 = new ObjectInputStream(f1);
//		Page p1 = (Page) in1.readObject();
//
//		//String dir2 = t.pages.get(1);
//
//		//FileInputStream f2 = new FileInputStream(dir2);
//		//ObjectInputStream in2 = new ObjectInputStream(f2);
//
//		//Page p2 = (Page) in2.readObject();
//
//		System.out.println(p1);
//		System.out.println("--------------------------------------\nEND\nOF\nPAGE\n--------------------------------------------");
		//System.out.println(p2);
		
		
		
		System.out.println("DELETING OCCURS HERE NOW -------------------------------");
		
		db.deleteFromTable("People", ht8);
		
		
		FileInputStream fi = new FileInputStream("data\\People_office.txt");
		ObjectInputStream oi = new ObjectInputStream(fi);
		BRTree<String> mybr = (BRTree<String>)oi.readObject();
		fi.close();
		oi.close();
		System.out.println(mybr);
		
		FileInputStream fi2 = new FileInputStream("data\\People_home.txt");
		ObjectInputStream oi2 = new ObjectInputStream(fi2);
		BRTree<String> mybr2 = (BRTree<String>)oi2.readObject();
		fi2.close();
		oi2.close();
		System.out.println(mybr2);

		
		System.out.println("THAT'S IT");

		// Getting page
		// Reading the object from a file
//        FileInputStream file = new FileInputStream("C:\\Users\\eiade\\Desktop\\People755991834.txt"); 
//        ObjectInputStream in = new ObjectInputStream(file); 

		// Method for deserialization of object

//        Page p = (Page)in.readObject(); 
//          
//        in.close(); 
//        file.close(); 
//          
//        System.out.println("Object has been deserialized "); 
//        System.out.println("N = " + p.N); 
//        System.out.println("size of a page = " + p.size());
//        for(int i = 0;i<p.size();i++) {
//        	System.out.println(((Tuple)p.get(i)).theTuple.get("name"));
//        }
		// done
		//Hashtable<String, Object> ht4 = new Hashtable<String, Object>();
	//	ht4.put("isHappy", false);
		//db.deleteFromTable("People", ht4);
//		
//		System.out.println("Num pages after ="+db.tables.get(0).pages.size());
	//	Hashtable<String, Object> ht5 = new Hashtable<String, Object>();
		//ht5.put("isHappy", false);
		//db.updateTable("People", "1", ht5);

//		int id; String name; boolean isHappy;
//		for(int i = 0 ; i<1000 ; i++) {
//			System.out.println("Adding tuple " + (i+1));
//			Random rand = new Random();
//			RandomString gen = new RandomString(8, ThreadLocalRandom.current());
//			int n = rand.nextInt(10000);
//			n+= 1;
//				id = n;
//				name =gen.nextString();
//				isHappy = (id%2 ==0);
//				
//				ht1.put("id", new Integer(id));
//				ht1.put("name", name);
//				ht1.put("gpa", new Double(n + 17.8));
//				ht1.put("isHappy", isHappy);
//				ht1.put("birthDay", "04/08/1999 03:30:04");
//				ht1.put("home", "(0,0),(1,1),(2,2)");
//				db.insertIntoTable("People", ht1);
//			
//				
//				
//			
//			}
		
		
		
		
	

	}

}
