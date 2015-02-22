import java.sql.*;

public class Main {

	static String databaseURL = "jdbc:postgresql://localhost:5432/health";
	static String databaseUser = "rajmohan";
	static String databasePassword = "";
	static Connection connection;
	static boolean [][] dataTypeCompatibilityMatrix;
	static int table1NoOfAttrs;
	static int table2NoOfAttrs;
	static String[] table1AttrTypes;
	static String[] table2AttrTypes;
	static String table1Name = "test3";
	static String table2Name = "test4";
	public static void init()
	{
		connectDB();
		table1NoOfAttrs = getNoOfAttributes(table1Name);
		table2NoOfAttrs =  getNoOfAttributes(table2Name);
//		System.out.println(noOfAttrInTable1 + "  " + noOfAttrInTable2);
		table1AttrTypes = new String[table1NoOfAttrs];
		table2AttrTypes = new String[table2NoOfAttrs];
		dataTypeCompatibilityMatrix = new boolean[table1NoOfAttrs][table2NoOfAttrs];
	}
	
	private static int getNoOfAttributes(String tableName) 
	{
		Statement st;
		ResultSet rs;
		try
		{
			st = connection.createStatement();
			rs = st.executeQuery("select count(column_name) from information_schema.columns where table_name = '" + tableName + "'");
			while(rs.next())
		    return Integer.parseInt(rs.getString(1)); 
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
		return -1;
	}
	
	public static void main(String... args)
	{
		init();
		ProcessData.doStep1();
		
//		printData();
	}
	
	static void printData()
	{
		Statement st;
		ResultSet rs;
		
		try 
		{
			int cnt = 0;
			st = connection.createStatement();
			rs = st.executeQuery("select column_name, data_type from information_schema.columns where table_name = 'test3' ");
			while (rs.next()) 
			{	
				cnt++;
				System.out.println(rs.getString(1) + "  " +rs.getString(2));//+" " +rs.getString(2)+" " +rs.getString(3) +" " +  rs.getString(3));
			}
			System.out.println("Count = " + cnt);
			rs.close();
			st.close();
		} 
		catch (SQLException e) 
		{
			e.printStackTrace();
		}		
	}
	
	static void connectDB()
	{
		try
		{
			Class.forName("org.postgresql.Driver");
			connection = DriverManager.getConnection(databaseURL, databaseUser, databasePassword);
			System.out.println("Connected to PgSQL!");
		}
		catch(Exception e)
		{
			System.out.println("Execption in ConnectDB: "+e);
		}
	}
	static void disconnectDB()
	{
		try
		{
			connection.close();
		}
		catch(Exception e)
		{
			System.out.println("Execption in ConnectDB: "+e);
			e.printStackTrace();
		}
	}
}

