import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;

public class ProcessDataMultiAttr {

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
	
	public static void doStep2()
	{
		double variance1 = computeVariance("Longitude", "test4");
		double variance2 = computeVariance("capacity", "test4");
		System.out.println("Var1 = "+ variance1 + ", Var2 = " + variance2);
	}
	
	private static double computeVariance(String attrName, String tableName) 
	{
		Statement st;
		ResultSet rs;
		double sqrdSum = 0.0, avg  = 0.0, tupCount = 0.0;
		try
		{
			st = Main.connection.createStatement();
			rs = st.executeQuery("select sum(" + attrName + " * " + attrName + "), avg(" + attrName + ""
					+ "), count(*) from " + tableName);
			
			while(rs.next())
			{
				sqrdSum = Double.parseDouble(rs.getString(1));
				avg = Double.parseDouble(rs.getString(2));
				tupCount = Double.parseDouble(rs.getString(3));
			}
			double var = (1/tupCount) * (sqrdSum - avg);
			rs.close();
			st.close();
			return var;
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		return -1;
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

	public static void Bifocal_Sampling() 
	{
		double m = 0.01;
		
		String source= "test3" ,sourceName="state";
		String target = "test1", targetName="state";
		
		List<String> sourceAttributes = Arrays.asList("city","county_name");
		List<String> targetAttributes = Arrays.asList("city","county_name");
		
		
		actualSupport(source, sourceAttributes, target, targetAttributes, m);
//		bifocalJoinSize(source, sourceName, target, targetName, m);
//		bifocalSupport(source, sourceName, target, targetName, m);
//		DenseSupport(source, sourceName, target, targetName, m);
		SparseSupport(source, sourceAttributes, target, targetAttributes, m);
		
	}
	
	private static  String getCommaSeperated(List<String> list) {
		StringBuilder result = new StringBuilder();
		for(String elm: list) {
			result.append(elm+",");
		}
		return result.substring(0, result.length()-1);
		
	}
	
	private static void actualSupport(String source, List<String> sourceAttribute,
			String target, List<String> targetAttribute, double m) 
	{
		if(sourceAttribute.size() == 2)
		{
			String querySource = "select count(*) from (select " + source + "." + sourceAttribute.get(0) + " , " + source + "." 
		      + sourceAttribute.get(1) + " from " + source +  " inner join ((select distinct " + targetAttribute.get(0) + " , " 
			  + targetAttribute.get(1) + " from " + target +  ")) table2  on " + source + "." + sourceAttribute.get(0) + " = " 
		      + "table2." + targetAttribute.get(0)+ " and " + source + "." + sourceAttribute.get(1) + " = " + "table2." 
			  + targetAttribute.get(1) + ") as djoin;";
			  
//		String querySource = "select count(*) from "+source+ " where "+ sourceAttribute +" in "+
//		"(select "+ targetAttribute+" from "+ target+" )";
		Statement st;
		ResultSet rs;

		try
		{
			st = Main.connection.createStatement();
			rs = st.executeQuery(querySource);
			
			if(rs.next())
			{
				System.out.println("Actual support = "+rs.getString(1));
				return;
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		}
		else
		{
			System.out.println("Attributes more than 3 as join attribs not supported yet!");
		}
	}

//	private static void bifocalJoinSize(String source, List<String> sourceAttribute,
//			String target, List<String> targetAttribute, double m) 
//	{
//	
//		double Ad = Dense_Dense_Estimation(source,sourceName,target,targetName,m);
//		double Ars = sparseAny(source,sourceName,target,targetName,m);
//		double Asr = sparseAny(target, targetName, source, sourceName, m);
//		
//		System.out.println("BifocalJoinSize Support = " + (Ad+Ars+Asr)+ " , Sampling: "+m);
//	}

//	private static void bifocalSupport(String source, List<String> sourceAttribute,
//			String target, List<String> targetAttribute, double m) 
//	{
//	
//		double Ad = Dense_Dense_Estimation(source,sourceName,target,targetName,m);
//		double Asas = sparseAnySupport(source,sourceName,target,targetName,m);
//		
//		System.out.println("BifocalSupport Support = " + (Ad+Asas)+ " , Sampling: "+m);
//	}
//	
//	private static void DenseSupport(String source, List<String> sourceAttribute,
//			String target, List<String> targetAttribute, double m) 
//	{
//	
//		double Ad = Dense_Dense_Estimation(source,sourceName,target,targetName,m);
//		
//		System.out.println("Sample Both Support = " + Ad+ " , Sampling: "+m);
//	}

	private static void SparseSupport(String source, List<String> sourceAttrNames,
			String target, List<String> targetAttrNames, double m) 
	{
	

		double Asas = sparseAnySupport(source,sourceAttrNames,target,targetAttrNames,m);
		
		System.out.println("Sample Source only Support = " + Asas+ " , Sampling: "+m);
	}

	private static double sparseAnySupport(String source,List<String> sourceAttrNames,String target,
			List<String> targetAttrNames,double m) 
	{
		double result = 0.0;
		double support = 0;
		String querySource = "select ";
		for(int i = 0; i < sourceAttrNames.size();i++)
		{
			querySource += sourceAttrNames.get(i);
			if(i < sourceAttrNames.size() - 1)
				querySource += " , ";
		}
		querySource += " from "+source+ " where random() < " + (double)m;
		
		String queryTarget = "select ";
		for(int i = 0; i < targetAttrNames.size();i++)
		{
			queryTarget += targetAttrNames.get(i);
			if(i < targetAttrNames.size() - 1)
				queryTarget += " , ";
		}
		queryTarget += " from "+target;
		
		List<ArrayList<String>> R;
		
		R = new ArrayList<ArrayList<String>>();
		
		List<ArrayList<String>> S = new ArrayList<ArrayList<String>>();
		Statement st;
		ResultSet rs;
		
		try
		{
			st = Main.connection.createStatement();
			rs = st.executeQuery(querySource);
			
			while(rs.next())
			{
				ArrayList<String> lst = new ArrayList<String>();
				lst.add(rs.getString(1));
				lst.add(rs.getString(2));
				R.add(lst);
			}
			rs.close();
			
			rs = st.executeQuery(queryTarget);
			
			while(rs.next())
			{
				ArrayList<String> lst = new ArrayList<String>();
				lst.add(rs.getString(1));
				lst.add(rs.getString(2));
				S.add(lst);
			}
			rs.close();
			st.close();
			
			for(int i = 0; i < R.size(); i++)
			{
				if(S.contains(R.get(i)))
					support++;
			}
			
			support = (1/m) * support;
			return support;
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		return result;
	}
	
	private static double sparseAny(String source,String sourceName,String target,
			String targetName,double m) 
	{
		double result = 0.0;
		double support = 0;
		String querySource = "select "+sourceName+" from "+source;
		String queryTarget = "select "+targetName+" from "+target+ " where random() < " + (double)m;
		ArrayList<String> R = new ArrayList<String>();
		ArrayList<String> S = new ArrayList<String>();
		Statement st;
		ResultSet rs;
		ArrayList<String> Vstar;

		try
		{
			st = Main.connection.createStatement();
			rs = st.executeQuery(querySource);
			
			while(rs.next())
			{
				R.add(rs.getString(1));
			}
			rs.close();
			
			rs = st.executeQuery(queryTarget);
			
			while(rs.next())
			{
				S.add(rs.getString(1));
			}
			rs.close();
			st.close();
			
			for(String str : S)
			{
				if(R.contains(str))
					support++;
			}
			
			support = (1/m) * support;
			return support;
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		return result;
		
	}
	

//	private static double Dense_Dense_Estimation(String source, List<String> sourceAttribute,
//			String target, List<String> targetAttribute, double m) 
//	{
//		double support;
//		ArrayList<String> sample1List = new ArrayList<String>();
//		ArrayList<String> sample2List = new ArrayList<String>();
//
//		String querySource = "select "+sourceName+" from "+source+ " where random() < " + (double)m;
//		String queryTarget = "select "+targetName+" from "+target+ " where random() < " + (double)m;
//		Statement st;
//		ResultSet rs;
//		ArrayList<String> Vstar;
//		try
//		{
//			st = Main.connection.createStatement();
//			rs = st.executeQuery(querySource);
//			
//			while(rs.next())
//			{
//				sample1List.add(rs.getString(1));
//			}
//			rs.close();
//			
//			rs = st.executeQuery(queryTarget);
//			
//			while(rs.next())
//			{
//				sample2List.add(rs.getString(1));
//			}
//			rs.close();
//			st.close();
//			Vstar = getIntersectedList(sample1List, sample2List);
//			support = (1/m) * Vstar.size();
//			return support;
//		}
//		catch(Exception e)
//		{
//			e.printStackTrace();
//		}
//		return -1.0;
//	}

//	private static double SampleBoth_Estimation(String source, List<String> sourceAttribute,
//			String target, List<String> targetAttribute, double m) 
//	{
//		double support = 0;
//		ArrayList<String> sample1List = new ArrayList<String>();
//		ArrayList<String> sample2List = new ArrayList<String>();
//
//		String querySource = "select "+sourceName+" from "+source+ " where random() < " + (double)m;
//		String queryTarget = "select "+targetName+" from "+target+ " where random() < " + (double)m;
//		Statement st;
//		ResultSet rs;
//		ArrayList<String> Vstar;
//		try
//		{
//			st = Main.connection.createStatement();
//			rs = st.executeQuery(querySource);
//			
//			while(rs.next())
//			{
//				sample1List.add(rs.getString(1));
//			}
//			rs.close();
//			
//			rs = st.executeQuery(queryTarget);
//			
//			while(rs.next())
//			{
//				sample2List.add(rs.getString(1));
//			}
//			rs.close();
//			st.close();
//			for(String s: sample1List)
//			{
//				if(sample2List.contains(s))
//					support++;
//			}
//			support = support * (1/m);
//			return support;
//		}
//		catch(Exception e)
//		{
//			e.printStackTrace();
//		}
//		return -1.0;
//	}
	private static ArrayList<String> getIntersectedList(
			ArrayList<String> sample1List, ArrayList<String> sample2List) 
	{
		ArrayList<String> resultList = new ArrayList<String>();
		
		for(String s: sample1List)
		{
			if(sample2List.contains(s))
				resultList.add(s);
		}
		
		return resultList;
	}	
}
