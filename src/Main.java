import java.sql.*;
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
    static List<Mapping> all;

    //CONFIGURATION PARAMETERS TO BE SET
    static String table1Name = "test3";
    static String table2Name = "test1";

    //Set these values.
    public static double srcSampleValue = 0.25;
    public static double destSampleValue = 0.25;
    public static double thresholdPerc = 0.2;

    //END OF CONFIGURATION PARAMETERS
    public static int expRunCnt = 1;
    //public static double fpProbability = 0.1;
    public static ResultFileWriter resFile;
    public static int targetRelSize,sourceRelSize;
    //public static int flag;
    public static boolean isActual;
    public static boolean debugMode = true;


    public static void init() {
        //Connects to database mentioned in 'databaseURL'
        connectDB();

        //Finds number of attributes from information_schema database.
        table1NoOfAttrs = getNoOfAttributes(table1Name);
        table2NoOfAttrs = getNoOfAttributes(table2Name);

        //Calculate source and relation size.
        sourceRelSize = getRelSize(table1Name);
        targetRelSize = getRelSize(table2Name);



        // file objects. Result will be written into these files.
        String vs = "Results/" +table1Name + "vs"+ table2Name +"/ssrc" + srcSampleValue
                + "sdest" + destSampleValue + "/t"+ thresholdPerc +"/";

        resFile = new ResultFileWriter(vs);

        table1AttrTypes = new String[table1NoOfAttrs];
        table2AttrTypes = new String[table2NoOfAttrs];
        table1AttrNames = new String[table1NoOfAttrs];
        table2AttrNames = new String[table2NoOfAttrs];
        all = new ArrayList<Mapping>();
        dataTypeCompatibilityMatrix = new boolean[table1NoOfAttrs][table2NoOfAttrs];
    }

    private static int getRelSize(String tableName) {
        Statement st;
        ResultSet rs;
        int cnt = -1;

        try {

            st = connection.createStatement();
            rs = st.executeQuery("select count(*) from " + tableName);

            if (rs.next()) {
                cnt = Integer.parseInt(rs.getString(1));
            }

            rs.close();
            st.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return cnt;
    }

    private static int getNoOfAttributes(String tableName) {
        Statement st;
        ResultSet rs;
        try {
            st = connection.createStatement();
            rs = st.executeQuery("select count(column_name) from information_schema.columns where table_name = '"
                    + tableName + "'");
            if (rs.next()) {
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

        isActual = true;
        ProcessData.doStep3Generic();

        isActual = false;
        ProcessData.doStep3Generic();

        // ProcessData.doStep2();
        // printData();
        // System.out.printf("\n\n");
        // ProcessData.doStep3WithActualSupport();

        // System.out.printf("\n\n");
        // ProcessData.doStep3WithBloom();
        // System.out.printf("\n\n");
        // ProcessData.doStep3WithTargetScaling();

        // System.out.printf("\n\n");
        // ProcessData.doStep3WithTargetScalingWithoutBloom();
        // ProcessData.Bifocal_Sampling();

        //Final closing operations should go here.
        close();
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

    static void close() {
        try {
            connection.close();
            resFile.close();
        } catch (Exception e) {
            System.out.println("Execption in ConnectDB: " + e);
            e.printStackTrace();
        }
    }
}
