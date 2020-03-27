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
		
		// if HT contains key with incompatible type throw exception
				Enumeration<String> enumeration = ht.keys();
				while (enumeration.hasMoreElements()) {
					String theName = enumeration.nextElement();
					if(!columnNames.contains(theName))
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
			p.deleteFromPage(ht);

			FileOutputStream fileSER = new FileOutputStream(pages.get(pageCounter));
			ObjectOutputStream outSER = new ObjectOutputStream(fileSER);
			outSER.writeObject(p);

			outSER.close();
			fileSER.close();
			System.out.println("Size after delete= " + p.size());
		}
	} // eficiency ya bro

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
