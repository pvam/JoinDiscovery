import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import orestes.bloomfilter.BloomFilter;
import orestes.bloomfilter.FilterBuilder;

public class ProcessData {

	public static void doStep1() {
		fillTableTypes(Main.table1Name, Main.table1AttrTypes);
		fillTableTypes(Main.table2Name, Main.table2AttrTypes);

		for (int i = 0; i < Main.table1NoOfAttrs; i++) {
			for (int j = 0; j < Main.table2NoOfAttrs; j++) {
				if (Main.table1AttrTypes[i] == Main.table2AttrTypes[j])
					Main.dataTypeCompatibilityMatrix[i][j] = true;
			}
		}
		print_matrix();
	}

	public static void doStep2() {
		double variance1 = computeVariance("Longitude", "test4");
		double variance2 = computeVariance("capacity", "test4");
		System.out.println("Var1 = " + variance1 + ", Var2 = " + variance2);
	}

	private static double computeVariance(String attrName, String tableName) {
		Statement st;
		ResultSet rs;
		double sqrdSum = 0.0, avg = 0.0, tupCount = 0.0;
		try {
			st = Main.connection.createStatement();
			rs = st.executeQuery("select sum(" + attrName + " * " + attrName
					+ "), avg(" + attrName + "" + "), count(*) from "
					+ tableName);

			while (rs.next()) {
				sqrdSum = Double.parseDouble(rs.getString(1));
				avg = Double.parseDouble(rs.getString(2));
				tupCount = Double.parseDouble(rs.getString(3));
			}
			double var = (1 / tupCount) * (sqrdSum - avg);
			rs.close();
			st.close();
			return var;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return -1;
	}

	private static void print_matrix() {
		int prunedOut = 0;
		for (int i = 0; i < Main.table1NoOfAttrs; i++) {
			for (int j = 0; j < Main.table2NoOfAttrs; j++) {
				System.out.print(Main.dataTypeCompatibilityMatrix[i][j] + "  ");
				if (!Main.dataTypeCompatibilityMatrix[i][j])
					prunedOut++;
			}
			System.out.println();
		}

		System.out
				.println("\nPruned Out Percentage = "
						+ 100
						* ((double) prunedOut / (Main.table1NoOfAttrs * Main.table2NoOfAttrs)));
	}

	private static void fillTableTypes(String tableName, String[] tableAttrTypes) {
		Statement st;
		ResultSet rs;
		int idx = 0;
		try {
			st = Main.connection.createStatement();
			rs = st.executeQuery("select data_type from information_schema.columns where table_name = '"
					+ tableName + "'");

			while (rs.next()) {
				String dType = rs.getString(1);

				// to handle character and character varying types
				if (dType.startsWith("character"))
					dType = "string";
				tableAttrTypes[idx++] = dType;
			}
			rs.close();
			st.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public static void Bifocal_Sampling() {
		double m = 0.1;

		String source = "test4", sourceName = "county";
		String target = "test3", targetName = "county_name";

		actualSupport(source, sourceName, target, targetName, m);
		bifocalSupport(source, sourceName, target, targetName, m);
		DenseSupport(source, sourceName, target, targetName, m);
		SparseSupport(source, sourceName, target, targetName, m);
		sparseSupportWithTargetScaling(source, sourceName, target, targetName,m);
		SampleBoth_Estimation(source, sourceName, target, targetName, m);
		
		System.out.println();
		sparseSupportWithBloomFilter(source, sourceName, target, targetName, m);
	}

	private static void sparseSupportWithBloomFilter(String source,
			String sourceName, String target, String targetName, double m) {
		double support = 0;
		String querySource = "select " + sourceName + " from " + source
				+ " where random() < " + (double) m;
		String queryTarget = "select " + targetName + " from " + target;
		ArrayList<String> R = new ArrayList<String>();

		BloomFilter<String> S = new FilterBuilder(20000, 0.1)
				.buildBloomFilter();

		Statement st;
		ResultSet rs;
		try {
			st = Main.connection.createStatement();
			rs = st.executeQuery(querySource);

			while (rs.next()) {
				R.add(rs.getString(1));
			}
			rs.close();

			rs = st.executeQuery(queryTarget);

			while (rs.next()) {
				String next = rs.getString(1);
				if(next != null)
				S.add(next);
			}
			rs.close();
			st.close();

			for (String str : R) {
				if (S.contains(str))
					support++;
			}

			support = (1 / m) * support;
			System.out.println("sparseSupportWithBloomFilter : " + support
					+ ", Sampling : " + m);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void sparseSupportWithTargetScaling(String source,
			String sourceName, String target, String targetName, double m) {
		double support = 0;
		ArrayList<String> sample1List = new ArrayList<String>();
		ArrayList<String> sample2List = new ArrayList<String>();

		String querySource = "select " + sourceName + " from " + source
				+ " where random() < " + m;
		String queryTarget = "select " + targetName + " from " + target
				+ " where random() < " + m;
		Statement st;
		ResultSet rs;
		ArrayList<String> Vstar;
		Set<String> disinctitem = new HashSet<String>();
		try {
			st = Main.connection.createStatement();
			rs = st.executeQuery(querySource);

			while (rs.next()) {
				sample1List.add(rs.getString(1));
			}
			rs.close();

			rs = st.executeQuery(queryTarget);

			while (rs.next()) {
				String next = rs.getString(1);
				sample2List.add(next);
				disinctitem.add(next);
			}

			for (String s : sample1List) {
				if (sample2List.contains(s))
					support++;
			}
			support = support * (1 / m);
			// Modify to also take target scaling into account

			int sampleDistinctSize = disinctitem.size();
			int totalDisinctSize = -1;
			// Number of distinct entries in whole column, can be obtained
			// through statistics of table.
			String distinctQuery = "select count(distinct " + targetName
					+ " ) from " + target;
			rs = st.executeQuery(distinctQuery);
			if (rs.next()) {
				totalDisinctSize = Integer.parseInt(rs.getString(1));
			}

			support *= (totalDisinctSize / sampleDistinctSize);
			System.out.println("sparseSupportWithTargetScaling :" + support
					+ ", sampling: " + m);

			rs.close();
			st.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	private static void actualSupport(String source, String sourceName,
			String target, String targetName, double m) {

		String querySource = "select count(*) from " + source + " where "
				+ sourceName + " in " + "(select " + targetName + " from "
				+ target + " )";
		Statement st;
		ResultSet rs;

		try {
			st = Main.connection.createStatement();
			rs = st.executeQuery(querySource);

			if (rs.next()) {
				System.out.println("Actual support = " + rs.getString(1));
				return;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void bifocalJoinSize(String target, String targetName,
			String source, String sourceName, double m) {

		double Ad = Dense_Dense_Estimation(source, sourceName, target,
				targetName, m);
		double Ars = sparseAny(source, sourceName, target, targetName, m);
		double Asr = sparseAny(target, targetName, source, sourceName, m);

		System.out.println("BifocalJoinSize Support = " + (Ad + Ars + Asr)
				+ " , Sampling: " + m);
	}

	private static void bifocalSupport(String source, String sourceName,
			String target, String targetName, double m) {

		double Ad = Dense_Dense_Estimation(source, sourceName, target,
				targetName, m);
		double Asas = sparseAnySupport(source, sourceName, target, targetName,
				m);

		System.out.println("BifocalSupport Support = " + (Ad + Asas)
				+ " , Sampling: " + m);
	}

	private static void DenseSupport(String source, String sourceName,
			String target, String targetName, double m) {

		double Ad = Dense_Dense_Estimation(source, sourceName, target,
				targetName, m);

		System.out.println("DenseSupport = " + Ad + " , Sampling: " + m);
	}

	private static void SparseSupport(String source, String sourceName,
			String target, String targetName, double m) {

		double Asas = sparseAnySupport(source, sourceName, target, targetName,
				m);

		System.out.println("Sample Source only Support = " + Asas
				+ " , Sampling: " + m);
	}

	private static double sparseAnySupport(String source, String sourceName,
			String target, String targetName, double m) {
		double result = 0.0;
		double support = 0;
		String querySource = "select " + sourceName + " from " + source
				+ " where random() < " + (double) m;
		String queryTarget = "select " + targetName + " from " + target;
		ArrayList<String> R = new ArrayList<String>();
		ArrayList<String> S = new ArrayList<String>();
		Statement st;
		ResultSet rs;
		ArrayList<String> Vstar;

		try {
			st = Main.connection.createStatement();
			rs = st.executeQuery(querySource);

			while (rs.next()) {
				R.add(rs.getString(1));
			}
			rs.close();

			rs = st.executeQuery(queryTarget);

			while (rs.next()) {
				S.add(rs.getString(1));
			}
			rs.close();
			st.close();

			for (String str : R) {
				if (S.contains(str))
					support++;
			}

			support = (1 / m) * support;
			return support;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	private static double sparseAny(String source, String sourceName,
			String target, String targetName, double m) {
		double result = 0.0;
		double support = 0;
		String querySource = "select " + sourceName + " from " + source;
		String queryTarget = "select " + targetName + " from " + target
				+ " where random() < " + (double) m;
		ArrayList<String> R = new ArrayList<String>();
		ArrayList<String> S = new ArrayList<String>();
		Statement st;
		ResultSet rs;
		ArrayList<String> Vstar;

		try {
			st = Main.connection.createStatement();
			rs = st.executeQuery(querySource);

			while (rs.next()) {
				R.add(rs.getString(1));
			}
			rs.close();

			rs = st.executeQuery(queryTarget);

			while (rs.next()) {
				S.add(rs.getString(1));
			}
			rs.close();
			st.close();

			for (String str : S) {
				if (R.contains(str))
					support++;
			}

			support = (1 / m) * support;
			return support;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;

	}

	private static double Dense_Dense_Estimation(String source,
			String sourceName, String target, String targetName, double m) {
		double support;
		ArrayList<String> sample1List = new ArrayList<String>();
		ArrayList<String> sample2List = new ArrayList<String>();

		String querySource = "select " + sourceName + " from " + source
				+ " where random() < " + (double) m;
		String queryTarget = "select " + targetName + " from " + target
				+ " where random() < " + (double) m;
		Statement st;
		ResultSet rs;
		ArrayList<String> Vstar;
		try {
			st = Main.connection.createStatement();
			rs = st.executeQuery(querySource);

			while (rs.next()) {
				sample1List.add(rs.getString(1));
			}
			rs.close();

			rs = st.executeQuery(queryTarget);

			while (rs.next()) {
				sample2List.add(rs.getString(1));
			}
			rs.close();
			st.close();
			Vstar = getIntersectedList(sample1List, sample2List);
			support = (1 / m) * Vstar.size();
			return support;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return -1.0;
	}

	private static void SampleBoth_Estimation(String source, String sourceName,
			String target, String targetName, double m) {
		double support = 0;
		ArrayList<String> sample1List = new ArrayList<String>();
		ArrayList<String> sample2List = new ArrayList<String>();

		String querySource = "select " + sourceName + " from " + source
				+ " where random() < " + (double) m;
		String queryTarget = "select " + targetName + " from " + target
				+ " where random() < " + (double) m;
		Statement st;
		ResultSet rs;
		ArrayList<String> Vstar;
		try {
			st = Main.connection.createStatement();
			rs = st.executeQuery(querySource);

			while (rs.next()) {
				sample1List.add(rs.getString(1));
			}
			rs.close();

			rs = st.executeQuery(queryTarget);

			while (rs.next()) {
				sample2List.add(rs.getString(1));
			}
			rs.close();
			st.close();
			for (String s : sample1List) {
				if (sample2List.contains(s))
					support++;
			}
			support = support * (1 / m);
			System.out.println("SampleBoth_Estimation " + support
					+ ", sampling:" + m);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static ArrayList<String> getIntersectedList(
			ArrayList<String> sample1List, ArrayList<String> sample2List) {
		ArrayList<String> resultList = new ArrayList<String>();

		for (String s : sample1List) {
			if (sample2List.contains(s))
				resultList.add(s);
		}

		return resultList;
	}
}
