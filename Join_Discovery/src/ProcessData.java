import java.sql.ResultSet;
import java.sql.Statement;

public class ProcessData {

	public static void doStep1()
	{
		fillTableTypes(Main.table1Name, Main.table1AttrTypes);
		fillTableTypes(Main.table2Name, Main.table2AttrTypes);
		
		for(int i = 0; i < Main.table1NoOfAttrs; i++)
		{
			for(int j = 0; j < Main.table2NoOfAttrs; j++)
			{
				if(Main.table1AttrTypes[i] == Main.table2AttrTypes[j])
					Main.dataTypeCompatibilityMatrix[i][j] = true; 
			}
		}
		print_matrix();	
	}
	
	private static void print_matrix() 
	{
		int prunedOut = 0;
		for(int i = 0; i < Main.table1NoOfAttrs; i++)
		{
			for(int j = 0; j < Main.table2NoOfAttrs; j++)
			{
				System.out.print(Main.dataTypeCompatibilityMatrix[i][j] + "  ");
				if(!Main.dataTypeCompatibilityMatrix[i][j])
					prunedOut++;
			}
			System.out.println();
		}
		
		System.out.println("\nPruned Out Percentage = " + 100 * ((double)prunedOut/(Main.table1NoOfAttrs * Main.table2NoOfAttrs)));
	}

	private static void fillTableTypes(String tableName, String[] tableAttrTypes) 
	{
		Statement st;
		ResultSet rs;
		int idx = 0;
		try
		{
			st = Main.connection.createStatement();
			rs = st.executeQuery("select data_type from information_schema.columns where table_name = '" + tableName + "'");
			
			while(rs.next())
			{
				String dType = rs.getString(1);
				
				// to handle character and character varying types
				if(dType.startsWith("character"))
					dType = "string";
				tableAttrTypes[idx++] = dType; 
			}
			rs.close();
			st.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
	}
	
}
