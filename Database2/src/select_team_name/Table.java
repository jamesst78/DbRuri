package select_team_name;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Random;
import java.util.StringTokenizer;

import bplus.BPTree;
import bplus.BRTree;
import bplus.Ref;

// 
public class Table implements Serializable {
	ArrayList<String> pages = new ArrayList<String>();
	String tableName;
	int N;

	public Table(String tableName, String tableKey, Hashtable ht) throws IOException {
		this.tableName = tableName;
		ArrayList<String> columnNames = new ArrayList<String>();
		ArrayList<String> columnTypes = new ArrayList<String>();
		Enumeration<String> enumeration = ht.keys();
		
		//Set N
		String row = "";
		BufferedReader csvReader = new BufferedReader(new FileReader("config\\DBApp.properties"));
		while ((row = csvReader.readLine()) != null) {
			String[] data = row.split("=");
			if(data[0].equals("MaximumRowsCountinPage"))
				N = Integer.parseInt(data[1]);
		}
		System.out.println(N);
		
		
		// iterate using enumeration object
		while (enumeration.hasMoreElements()) {

			String key = enumeration.nextElement();
			columnNames.add(key);
			columnTypes.add((String) ht.get(key));
		}
		// DateFormat df = new SimpleDateFormat("dd/MM/yy HH:mm:ss");
		// Date dateobj = new Date();
		columnNames.add("TouchDate");
		columnTypes.add("java.util.Date");

		FileWriter myWriter = new FileWriter("data\\metadata.csv", true);
		for (int j = 0; j < columnNames.size(); j++)
			myWriter.write(tableName + "," + columnNames.get(j) + "," + columnTypes.get(j) + ","
					+ columnNames.get(j).equals(tableKey) + ",false\n");
		myWriter.close();

		String directory = "data\\" + tableName + ".txt";
		File file = new File(directory);
		file.createNewFile();
		FileOutputStream fileO = new FileOutputStream(directory, true);
		ObjectOutputStream out = new ObjectOutputStream(fileO);
		out.writeObject(this);

		out.close();
		fileO.close();
	}

	public void insertIntoTable(String tableName, Hashtable<String, Comparable> ht)
			throws IOException, ClassNotFoundException {
		SimpleDateFormat formatter = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
		Date date = new Date();
		ht.put("TouchDate", date);
		//TODO REMOVE LATER
		String tableKey = "id";
		Tuple t = new Tuple(ht, tableKey);
		if (pages.size() == 0) {
			System.out.println("test2");
			Random random = new Random();
			int r = random.nextInt(2147000000);
			String directory = "data\\" + tableName + r + ".txt";
			Page p = new Page(t, N, tableKey);
			File file = new File(directory);
			file.createNewFile();
			FileOutputStream fileO = new FileOutputStream(directory, true);
			ObjectOutputStream out = new ObjectOutputStream(fileO);
			pages.add(directory);
			out.writeObject(p);

			out.close();
			fileO.close();

		} else {
			int pageCounter = 0;
			do {
				FileInputStream file = new FileInputStream(pages.get(pageCounter));
				ObjectInputStream in = new ObjectInputStream(file);

				// Method for deserialization of object
				Page p = (Page) in.readObject();
				in.close();
				file.close();
				boolean lastPage = pageCounter == pages.size() - 1;
				System.out.println("in");
				t = p.insertIntoPage(t, lastPage);
				System.out.println("out");
				if (!(t == null) && lastPage) {
					// START

					Random random = new Random();
					int r = random.nextInt(2147000000);
					String directory = "data\\" + tableName + r + ".txt";
					Page pLast = new Page(t, N, tableKey);
					File fileLast = new File(directory);
					fileLast.createNewFile();
					FileOutputStream fileO = new FileOutputStream(directory, true);
					ObjectOutputStream out = new ObjectOutputStream(fileO);
					pages.add(directory);
					out.writeObject(pLast);

					out.close();
					fileO.close();

					t = null;

					// END
				}

				// START SER USED PAGE

				FileOutputStream fileSER = new FileOutputStream(pages.get(pageCounter));
				ObjectOutputStream outSER = new ObjectOutputStream(fileSER);
				outSER.writeObject(p);

				outSER.close();
				fileSER.close();

				// END SER USED PAGE

				pageCounter++;

			} while (!(t == null));
		}
	}
	
	public void deleteFromTableBTree(String strTableName, Hashtable<String, Comparable> ht,ArrayList<BRTree<String>> bRAllht,
			ArrayList<String> RPlusColht) throws IOException, ClassNotFoundException, DBAppException{
		String type = "";
		String tableKey = "";
		ArrayList<String> columnNames = new ArrayList<String>();
		ArrayList<String> columnTypes = new ArrayList<String>();
		String row = "";
		BufferedReader csvReader = new BufferedReader(new FileReader("data\\metadata.csv"));
		while ((row = csvReader.readLine()) != null) {
			String[] data = row.split(",");
			// do something with the data
			if (data[0].equals(tableName)) {
				columnNames.add(data[1]);
				columnTypes.add(data[2]);
				if (data[3].equals("true")) {
					type = data[2];
					tableKey = data[1];
				}
			}
		}
		//trees belonging to this table
		ArrayList<BRTree<String>> bRAll = new ArrayList<BRTree<String>>();
		ArrayList<String> correspondingColumns = new ArrayList<String>();
		BufferedReader mCsvReader = new BufferedReader(new FileReader("data\\metadata.csv"));
		while ((row = mCsvReader.readLine()) != null) {
			String[] data = row.split(",");
			// do something with the data
			if (data[0].equals(tableName) && data[4].equals("true")) {

				String tPath = "data/" + data[0] + "_" + data[1] + ".txt";
				FileInputStream fTree = new FileInputStream(tPath);
				ObjectInputStream inTree = new ObjectInputStream(fTree);
				BRTree<String> bP = (BRTree<String>) inTree.readObject();
				bRAll.add(bP);
				correspondingColumns.add(data[1]);
				fTree.close();
				inTree.close();
			}
		}

		// if HT contains key with incompatible type throw exception
		Enumeration<String> enumeration = ht.keys();
		while (enumeration.hasMoreElements()) {
			String theName = enumeration.nextElement();
			if (!columnNames.contains(theName))
				throw new DBAppException("Table does not contain column named " + theName);
			Comparable theValue = ht.get(theName);
			String expectedType = "";
			int i = 0;
			for (i = 0; i < columnNames.size(); i++)
				if (columnNames.get(i).equals(theName)) {
					expectedType = columnTypes.get(i);
					break;
				}

			switch (expectedType) {
			case "java.lang.Integer":
				if (!theValue.getClass().toString().equals("class java.lang.Integer"))
					throw new DBAppException("Incorrect type entered .. " + theName + " Should be Integer");
				break;
			case "java.lang.Double":
				if (!theValue.getClass().toString().equals("class java.lang.Double"))
					throw new DBAppException("Incorrect type entered .. " + theName + " Should be Double");
				break;
			case "java.lang.String":
				if (!theValue.getClass().toString().equals("class java.lang.String"))
					throw new DBAppException("Incorrect type entered .. " + theName + " Should be String");
				break;
			case "java.awt.Polygon":
				ht.put(columnNames.get(i), Polygon.parsePolygon(theValue.toString()));
				break;
			case "java.util.Date":
				SimpleDateFormat formatter = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
				try {
					ht.put(columnNames.get(i), formatter.parse(theValue.toString()));
				} catch (ParseException e) {
					throw new DBAppException("Invalid date format entered");
				}
				break;
			case "java.lang.Boolean":
				if (!theValue.getClass().toString().equals("class java.lang.Boolean"))
					throw new DBAppException("Incorrect type entered .. " + theName + " Should be Boolean");
				break;
			}
		}
			boolean tobeincluded = true;
			ArrayList<ArrayList <Ref>> allRefs = new ArrayList();
	     	//ArrayList<Ref> refs = bRAllht.get(0).search(RPlusColht.get(0));
	     	ArrayList<Ref> refsToBeDeleted = new ArrayList<Ref>();
            for(int i=0;i< bRAllht.size();i++){
            	allRefs.get(i).addAll(bRAllht.get(i).search(RPlusColht.get(i)));  //got all refs in all the trees
            }
            
            //check ref by ref if its in all other trees
            Ref chosenRef;
            ArrayList<Ref> chosenList;
            for(int j = 0 ; j<allRefs.size() ; j++) {
            	//choose a list first
            	chosenList = allRefs.get(j);
            	//now check ref by ref in the chosen list
            		for(int k = 0 ; k <chosenList.size() ; k++) {
            			tobeincluded = true;
            			chosenRef = chosenList.get(k);
            			//now check if the other lists contain it
            					for(int p = 0 ; p<allRefs.size(); p++) {
            						if(!allRefs.get(p).contains(chosenRef)) {
            							tobeincluded = false;
            						}
            					}
            				if(tobeincluded == true) {
            					refsToBeDeleted.add(chosenRef);
            				}
            			
            					
            				
            		}
            	
            }
			
			ArrayList<String> finishedPages = new ArrayList<String>();
			for (Ref ref : refsToBeDeleted) {
				if (finishedPages.contains(ref.getPage()))
					continue;
				String page = ref.getPage();
				finishedPages.add(page);
				System.out.println(page);
				FileInputStream pfile = new FileInputStream(page);
				ObjectInputStream pin = new ObjectInputStream(pfile);

				Page p = (Page) pin.readObject();
				p.deleteFromPage(ht, bRAll, correspondingColumns, ref.getPage());

				pfile.close();
				pin.close();

				FileOutputStream fileSER = new FileOutputStream(page);
				ObjectOutputStream outSER = new ObjectOutputStream(fileSER);
				outSER.writeObject(p);

				outSER.close();
				fileSER.close();
				//System.out.println("Size after delete= " + p.size());
			}
		
		
		// serialize the btrees 
		for (int x = 0; x < bRAll.size(); x++) {
			FileOutputStream fo = new FileOutputStream("data/" + tableName + "_" + correspondingColumns.get(x) + ".txt");
			ObjectOutputStream oj = new ObjectOutputStream(fo);
			oj.writeObject(bRAll.get(x));
			fo.close();
			oj.close();
		}
	}


	public void deleteFromTable(String strTableName, Hashtable<String, Comparable> ht)
			throws IOException, ClassNotFoundException, DBAppException {
		String type = "";
		String tableKey = "";
		ArrayList<String> columnNames = new ArrayList<String>();
		ArrayList<String> columnTypes = new ArrayList<String>();
		String row = "";
		BufferedReader csvReader = new BufferedReader(new FileReader("data\\metadata.csv"));
		while ((row = csvReader.readLine()) != null) {
			String[] data = row.split(",");
			// do something with the data
			if (data[0].equals(tableName)) {
				columnNames.add(data[1]);
				columnTypes.add(data[2]);
				if (data[3].equals("true")) {
					type = data[2];
					tableKey = data[1];
				}
			}
		}
		//trees belonging to this table
		ArrayList<BRTree<String>> bRAll = new ArrayList<BRTree<String>>();
		ArrayList<String> correspondingColumns = new ArrayList<String>();
		BufferedReader mCsvReader = new BufferedReader(new FileReader("data\\metadata.csv"));
		while ((row = mCsvReader.readLine()) != null) {
			String[] data = row.split(",");
			// do something with the data
			if (data[0].equals(tableName) && data[4].equals("true")) {

				String tPath = "data/" + data[0] + "_" + data[1] + ".txt";
				FileInputStream fTree = new FileInputStream(tPath);
				ObjectInputStream inTree = new ObjectInputStream(fTree);
				BRTree<String> bR = (BRTree<String>) inTree.readObject();
				bRAll.add(bR);
				correspondingColumns.add(data[1]);
				fTree.close();
				inTree.close();
			}
		}

		// if HT contains key with incompatible type throw exception
		Enumeration<String> enumeration = ht.keys();
		while (enumeration.hasMoreElements()) {
			String theName = enumeration.nextElement();
			if (!columnNames.contains(theName))
				throw new DBAppException("Table does not contain column named " + theName);
			Comparable theValue = ht.get(theName);
			String expectedType = "";
			int i = 0;
			for (i = 0; i < columnNames.size(); i++)
				if (columnNames.get(i).equals(theName)) {
					expectedType = columnTypes.get(i);
					break;
				}

			switch (expectedType) {
			case "java.lang.Integer":
				if (!theValue.getClass().toString().equals("class java.lang.Integer"))
					throw new DBAppException("Incorrect type entered .. " + theName + " Should be Integer");
				break;
			case "java.lang.Double":
				if (!theValue.getClass().toString().equals("class java.lang.Double"))
					throw new DBAppException("Incorrect type entered .. " + theName + " Should be Double");
				break;
			case "java.lang.String":
				if (!theValue.getClass().toString().equals("class java.lang.String"))
					throw new DBAppException("Incorrect type entered .. " + theName + " Should be String");
				break;
			case "java.awt.Polygon":
				ht.put(columnNames.get(i), Polygon.parsePolygon(theValue.toString()));
				break;
			case "java.util.Date":
				SimpleDateFormat formatter = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
				try {
					ht.put(columnNames.get(i), formatter.parse(theValue.toString()));
				} catch (ParseException e) {
					throw new DBAppException("Invalid date format entered");
				}
				break;
			case "java.lang.Boolean":
				if (!theValue.getClass().toString().equals("class java.lang.Boolean"))
					throw new DBAppException("Incorrect type entered .. " + theName + " Should be Boolean");
				break;
			}
		}

		for (int pageCounter = 0; pageCounter < pages.size(); pageCounter++) {
			FileInputStream file = new FileInputStream(pages.get(pageCounter));
			ObjectInputStream in = new ObjectInputStream(file);

			// Method for deserialization of object
			Page p = (Page) in.readObject();
			in.close();
			file.close();
			System.out.println("Size before delete= " + p.size());
			p.deleteFromPage(ht, bRAll, correspondingColumns, pages.get(pageCounter));

			FileOutputStream fileSER = new FileOutputStream(pages.get(pageCounter));
			ObjectOutputStream outSER = new ObjectOutputStream(fileSER);
			outSER.writeObject(p);

			outSER.close();
			fileSER.close();
			System.out.println("Size after delete= " + p.size());
		}
		// serialize the btrees 
		for (int x = 0; x < bRAll.size(); x++) {
			FileOutputStream fo = new FileOutputStream("data/" + tableName + "_" + correspondingColumns.get(x) + ".txt");
			ObjectOutputStream oj = new ObjectOutputStream(fo);
			oj.writeObject(bRAll.get(x));
			fo.close();
			oj.close();
		}
	}

//	public static void main(String[] args) throws IOException {
//		Hashtable<String, String> ht = new Hashtable<String, String>();
//		ht.put("id", "java.lang.Integer");
//		ht.put("name", "java.lang.String");
//		ht.put("gpa", "java.lang.double");
//		Table table = createTable("Student", "id", ht);
//		Table t = createTable("People", "id", ht);
//		Hashtable<String, Comparable> theTuple = new Hashtable<String, Comparable>();
//		theTuple.put("id", new Integer(1));
//		theTuple.put("name", new String("Mohab"));
//		theTuple.put("gpa", new Double(0.99));
//		t.insertIntoTable("People", theTuple);
//		System.out.println("Hi");
//	
//} nbos 3leha hhhhhhhbm best effort service +1 farah :P

	// tdf3y kam??? oh yeah wt f*** :)
	public void updateTable(String strTableName, String key, Hashtable<String, Comparable> ht)
			throws IOException, ClassNotFoundException, DBAppException {
		// Get names types, and key from metadata
		String type = "";
		String tableKey = "";
		ArrayList<String> columnNames = new ArrayList<String>();
		ArrayList<String> columnTypes = new ArrayList<String>();
		String row = "";
		BufferedReader csvReader = new BufferedReader(new FileReader("data\\metadata.csv"));
		while ((row = csvReader.readLine()) != null) {
			String[] data = row.split(",");
			// do something with the data
			if (data[0].equals(tableName)) {
				columnNames.add(data[1]);
				columnTypes.add(data[2]);
				if (data[3].equals("true")) {
					type = data[2];
					tableKey = data[1];
				}
			}
		}
		// if key is in HT throw exception
		System.out.println(tableKey);
		if (ht.containsKey(tableKey)) {
			throw new DBAppException("Cannot change value of clustering key");
		}

		// if HT contains key not in table throw exception
		Enumeration<String> enumeration = ht.keys();
		while (enumeration.hasMoreElements()) {

			String key0 = enumeration.nextElement();
			if (!columnNames.contains(key0))
				throw new DBAppException("Table does not contain column named " + key0);
		}
		
		// if HT contains key with incompatible type throw exception
		enumeration = ht.keys();
		while (enumeration.hasMoreElements()) {

			String theName = enumeration.nextElement();
			Comparable theValue = ht.get(theName);
			String expectedType = "";
			int i =0;
			for (i = 0; i < columnNames.size(); i++)
				if (columnNames.get(i).equals(theName)) {
					expectedType = columnTypes.get(i);
					break;
				}

			switch (expectedType) {
			case "java.lang.Integer":
				if (!theValue.getClass().toString().equals("class java.lang.Integer"))
					throw new DBAppException("Incorrect type entered .. " + theName + " Should be Integer");
				break;
			case "java.lang.Double":
				if (!theValue.getClass().toString().equals("class java.lang.Double"))
					throw new DBAppException("Incorrect type entered .. " + theName + " Should be Double");
				break;
			case "java.lang.String":
				if (!theValue.getClass().toString().equals("class java.lang.String"))
					throw new DBAppException("Incorrect type entered .. " + theName + " Should be String");
				break;
			case "java.awt.Polygon":
				ht.put(columnNames.get(i), Polygon.parsePolygon(theValue.toString()));
				break;
			case "java.util.Date":
				SimpleDateFormat dformatter = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
				try {
					System.out.println(theValue.toString());
					ht.put(columnNames.get(i), dformatter.parse(theValue.toString()));
				} catch (ParseException e) {
					throw new DBAppException("Invalid date format entered");
				}
				break;
			case "java.lang.Boolean":
				if (!theValue.getClass().toString().equals("class java.lang.Boolean"))
					throw new DBAppException("Incorrect type entered .. " + theName + " Should be Boolean");
				break;
			}
		}
		SimpleDateFormat formatter = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
		Date date = new Date();
		ht.put("TouchDate", date);

		Comparable a = null;
		switch (type) {
		case "java.lang.Integer":
			try {
				a = Integer.parseInt(key);
			} catch (NumberFormatException e) {
				throw new DBAppException("Incorrect type entered .. Should be Integer");
			}
			break;
		case "java.lang.Double":
			try {
				a = Double.parseDouble(key);
			} catch (NumberFormatException e) {
				throw new DBAppException("Incorrect type entered .. Should be Double");
			}
			break;
		case "java.lang.String":
			a = key;
		case "java.awt.Polygon":
			a = Polygon.parsePolygon(key);
			break;
		case "java.util.Date":
			SimpleDateFormat dformatter = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
			try {
				a = formatter.parse(key);
			} catch (ParseException e) {
				throw new DBAppException("Invalid date format entered");
			}
			break;
		case "java.lang.Boolean":
			if (!(key.equals("false") || key.equals("true")))
				throw new DBAppException("Incorrect type entered .. Should be Boolean");
			a = Boolean.parseBoolean(key);
			break;
		}
		int pageCounter = 0;
		do {
			// Method for deserialization of object

			FileInputStream file = new FileInputStream(pages.get(pageCounter));
			ObjectInputStream in = new ObjectInputStream(file);

			Page p = (Page) in.readObject();

			p.updatePage(tableName, a, ht);
			in.close();
			file.close();

			// Method for serialization of object
			FileOutputStream fileSER = new FileOutputStream(pages.get(pageCounter));
			ObjectOutputStream outSER = new ObjectOutputStream(fileSER);
			outSER.writeObject(p);

			outSER.close();
			fileSER.close();
			pageCounter++;
		} while (pageCounter < pages.size());
		csvReader.close();

		for (int pajeCounter = 0; pajeCounter < pages.size(); pajeCounter++) {
			FileInputStream file = new FileInputStream(pages.get(pajeCounter));
			ObjectInputStream in = new ObjectInputStream(file);

			// Method for deserialization of object
			Page p = (Page) in.readObject();
			// to be implemented VV
			// p.updatePage(ht);

			FileOutputStream fileSER = new FileOutputStream(pages.get(pajeCounter));
			ObjectOutputStream outSER = new ObjectOutputStream(fileSER);
			outSER.writeObject(p);

			outSER.close();
			fileSER.close();

		}
	}

}
