package select_team_name;



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
import java.util.concurrent.ThreadLocalRandom;

import com.github.davidmoten.rtree.RTree;
import com.github.davidmoten.rtree.Serializer;
import com.github.davidmoten.rtree.Serializers;
import com.github.davidmoten.rtree.geometry.Geometry;

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
	}

	public void deleteFromTable(String strTableName, Hashtable<String, Object> ht)
			throws ClassNotFoundException, IOException, DBAppException {
		String dir = "data\\" + strTableName + ".txt";
		String row = "";
		BufferedReader csvReader = new BufferedReader(new FileReader("data\\metadata.csv"));
		boolean flag = false;
		while ((row = csvReader.readLine()) != null) {
			String[] data = row.split(",");
			// do something with the data
			if (data[0].equals(strTableName)) {
				flag = true;
			}
		}
		if(!flag)
			throw new DBAppException("Table already exists");
		Hashtable<String, Comparable> newHt = new Hashtable<String, Comparable>();
		Enumeration<String> enumeration = ht.keys();
		// iterate using enumeration object
		while (enumeration.hasMoreElements()) {

			String htKey = enumeration.nextElement();
			newHt.put(htKey, (Comparable) ht.get(htKey));
		}
		// deserialize
		FileInputStream file = new FileInputStream(dir);
		ObjectInputStream in = new ObjectInputStream(file);

		// Method for deserialization of object
		Table t = (Table) in.readObject();

		t.deleteFromTable(strTableName, newHt);
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
		FileOutputStream fileO = new FileOutputStream(dir);
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
	
	
	// Muhad && Farah R tree
	
	public void createRTreeIndex(String strTableName , String strColName) throws DBAppException, IOException, ClassNotFoundException
	{
		
		RTree<String, Geometry> tree = RTree.create();
		//han assume el Column type is a polygon f3lan
		
		//we shall go deserialize el table
		String pageString = "";
		String dir = "data\\" + strTableName + ".txt";
		String treeDir = "data\\" + strTableName + strColName + "RTreeIndex" + ".txt";
		FileInputStream file = new FileInputStream(dir);
		ObjectInputStream in = new ObjectInputStream(file);
		
		Table t = (Table) in.readObject();
		
		for(int i = 0 ; i<t.pages.size() ; i++) {
			pageString = t.pages.get(i);
			FileInputStream file2 = new FileInputStream(pageString);
			ObjectInputStream in2 = new ObjectInputStream(file2);
			//do something with the page
			Page p = (Page) in2.readObject();
			tree = p.fillRTree(tree , strColName, t.pages.get(i));
			//
			in2.close();
			file2.close();
			
			//serialize the page back
			FileOutputStream fileO = new FileOutputStream(pageString);
			ObjectOutputStream out = new ObjectOutputStream(fileO);
			out.writeObject(t);

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
		
		//TODO : Serialize the tree into a file
		
		OutputStream file4 = new FileOutputStream(treeDir);
		
		Serializer<String, Geometry> serializer =  Serializers.flatBuffers().utf8();
				serializer.write(tree, file4);
		
		
		
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

		Hashtable<String, Object> ht1 = new Hashtable<String, Object>();
		ht1.put("id", new Integer(1));
		ht1.put("name", "Eiad");
		//ht1.put("gpa", 0.7);
	//	ht1.put("isHappy", false);
		//ht1.put("birthDay", "04/08/1999 03:30:04");
		ht1.put("home", "(0,0),(1,1),(2,2)");
		

		Hashtable<String, Object> ht2 = new Hashtable<String, Object>();
		ht2.put("id", new Integer(3));
		ht2.put("name", "Mohab");
		ht2.put("home", "(0,0),(5,5),(2,2)");

		Hashtable<String, Object> ht3 = new Hashtable<String, Object>();
		ht3.put("id", new Integer(2));
		ht3.put("name", "47");
		ht3.put("home", "(0,0),(5,5),(2,2)");
		db.createTable("People", "id", ht);
		db.insertIntoTable("People", ht1);
		db.insertIntoTable("People", ht2);
		db.insertIntoTable("People", ht3);
		
		db.createRTreeIndex("People", "home");

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
		Hashtable<String, Object> ht4 = new Hashtable<String, Object>();
		ht4.put("isHappy", false);
		//db.deleteFromTable("People", ht4);
//		
//		System.out.println("Num pages after ="+db.tables.get(0).pages.size());
		Hashtable<String, Object> ht5 = new Hashtable<String, Object>();
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
		
		
		
		
		String dir = "data\\People.txt";
		FileInputStream f = new FileInputStream(dir);
		ObjectInputStream in = new ObjectInputStream(f);

		Table t = (Table) in.readObject();
		
		String dir1 = t.pages.get(0);

		FileInputStream f1 = new FileInputStream(dir1);
		ObjectInputStream in1 = new ObjectInputStream(f1);
		Page p1 = (Page) in1.readObject();

		String dir2 = t.pages.get(1);

		FileInputStream f2 = new FileInputStream(dir2);
		ObjectInputStream in2 = new ObjectInputStream(f2);

		Page p2 = (Page) in2.readObject();

//		System.out.println(p1);
//		System.out.println("--------------------------------------\nEND\nOF\nPAGE\n--------------------------------------------");
//		System.out.println(p2);

	}

}
