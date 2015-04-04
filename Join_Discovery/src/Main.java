import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class Main {

	static String databaseURL = "jdbc:postgresql://localhost:5432/health";
	static String databaseUser = "postgres";
	static String databasePassword = "a";
	static Connection connection;
	static boolean[][] dataTypeCompatibilityMatrix;
	static int table1NoOfAttrs;
	static int table2NoOfAttrs;
	static String[] table1AttrTypes, table1AttrNames;
	static String[] table2AttrTypes, table2AttrNames;

	static String table1Name = "customer";
	static String table2Name = "orders";

	static List<Pair> all;
	public static double fpProbability = 0.1;
	public static int targetrelSize = 1500000;
	public static int threshold = 15000;// (int) (targetrelSize * 0.1);
	public static double sampleValue = 0.2;
	public static final boolean debugMode = true;

	public static void init() {
		connectDB();
		table1NoOfAttrs = getNoOfAttributes(table1Name);
		table2NoOfAttrs = getNoOfAttributes(table2Name);
		// System.out.println(noOfAttrInTable1 + "  " + noOfAttrInTable2);
		table1AttrTypes = new String[table1NoOfAttrs];
		table2AttrTypes = new String[table2NoOfAttrs];
		table1AttrNames = new String[table1NoOfAttrs];
		table2AttrNames = new String[table2NoOfAttrs];
		all = new ArrayList<Pair>();
		dataTypeCompatibilityMatrix = new boolean[table1NoOfAttrs][table2NoOfAttrs];
	}

	private static int getNoOfAttributes(String tableName) {
		Statement st;
		ResultSet rs;
		try {
			st = connection.createStatement();
			rs = st.executeQuery("select count(column_name) from information_schema.columns where table_name = '"
					+ tableName + "'");
			while (rs.next()) {
				return Integer.parseInt(rs.getString(1));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return -1;
	}

	public static void main(String... args) {
		init();
		ProcessData.doStep1();
		// TODO: Yet to be implemented
		// ProcessData.doStep2();
		// printData();
		System.out.printf("\n\n");
		// ProcessData.doStep3WithActualSupport();

		System.out.printf("\n\n");
		// ProcessData.doStep3WithBloom();
		System.out.printf("\n\n");
		ProcessData.doStep3WithTargetScaling();

		System.out.printf("\n\n");
		// ProcessData.doStep3WithTargetScalingWithoutBloom();
		// ProcessData.Bifocal_Sampling();
	}

	static void printData() {
		Statement st;
		ResultSet rs;

		try {
			int cnt = 0;
			st = connection.createStatement();
			rs = st.executeQuery("select column_name, data_type from information_schema.columns where table_name = 'test3' ");
			while (rs.next()) {
				cnt++;
				System.out.println(rs.getString(1) + "  " + rs.getString(2));// +" "
				// +rs.getString(2)+" "
				// +rs.getString(3)
				// +" "
				// +
				// rs.getString(3));
			}
			System.out.println("Count = " + cnt);
			rs.close();
			st.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	static void connectDB() {
		try {
			Class.forName("org.postgresql.Driver");
			connection = DriverManager.getConnection(databaseURL, databaseUser,
					databasePassword);
			System.out.println("Connected to PgSQL!");
		} catch (Exception e) {
			System.out.println("Execption in ConnectDB: " + e);
		}
	}

	static void disconnectDB() {
		try {
			connection.close();
		} catch (Exception e) {
			System.out.println("Execption in ConnectDB: " + e);
			e.printStackTrace();
		}
	}
}
