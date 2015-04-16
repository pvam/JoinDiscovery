import orestes.bloomfilter.BloomFilter;
import orestes.bloomfilter.FilterBuilder;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;

public class ProcessData {
    static ArrayList<ItemSet> candidateItemSetList = new ArrayList<ItemSet>();
    static ArrayList<ItemSet> globalFreqItemSetList = new ArrayList<ItemSet>();

    public static void doStep1() {
        fillTableTypes(Main.table1Name, Main.table1AttrTypes,
                Main.table1AttrNames);
        fillTableTypes(Main.table2Name, Main.table2AttrTypes,
                Main.table2AttrNames);

        for (int i = 0; i < Main.table1NoOfAttrs; i++) {
            for (int j = 0; j < Main.table2NoOfAttrs; j++) {
                if (Main.table1AttrTypes[i].equalsIgnoreCase(Main.table2AttrTypes[j])) {
                    Main.dataTypeCompatibilityMatrix[i][j] = true;
                }
            }
        }
        if (Main.debugMode)
            print_matrix();
    }

    // Not used.
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
        System.out.println("Source Attributes List");
        for (int i = 0; i < Main.table1NoOfAttrs; ++i) {
            System.out.print(Main.table1AttrNames[i] + "  ");
        }
        System.out.println();
        System.out.println();
        System.out.println("Target Attributes List");
        for (int i = 0; i < Main.table2NoOfAttrs; ++i) {
            System.out.print(Main.table2AttrNames[i] + "  ");
        }
        System.out.println();
        System.out.println();
        System.out.println("Datatype compatibility matrix");
        for (int i = 0; i < Main.table1NoOfAttrs; i++) {
            for (int j = 0; j < Main.table2NoOfAttrs; j++) {
                System.out.print(Main.dataTypeCompatibilityMatrix[i][j] ? "T "
                        : "F ");
                if (!Main.dataTypeCompatibilityMatrix[i][j]) {
                    prunedOut++;
                }
            }
            System.out.println();
        }

        System.out
                .println("\nPruned Out Percentage = "
                        + 100
                        * ((double) prunedOut / (Main.table1NoOfAttrs * Main.table2NoOfAttrs)));
    }

    private static void fillTableTypes(String tableName,
                                       String[] tableAttrTypes, String[] tableAttrNames) {
        Statement st;
        ResultSet rs;
        int idx = 0;
        try {
            st = Main.connection.createStatement();
            rs = st.executeQuery("select data_type,column_name from information_schema.columns where table_name = '"
                    + tableName + "'");

            while (rs.next()) {
                String dType = rs.getString(1);
                String attrName = rs.getString(2);

                // to handle character and character varying types
                if (dType.startsWith("character")) {
                    dType = "string";
                }
                tableAttrTypes[idx] = dType;
                tableAttrNames[idx] = attrName;
                ++idx;
            }
            rs.close();
            st.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static void Bifocal_Sampling() {
        double m = Main.sampleValue;

        String source = "test4", sourceName = "county";
        String target = "test3", targetName = "county_name";

        actualSupport(source, sourceName, target, targetName, m);
        bifocalSupport(source, sourceName, target, targetName, m);
        DenseSupport(source, sourceName, target, targetName, m);
        SparseSupport(source, sourceName, target, targetName, m);
        //sparseSupportWithTargetScaling(source, sourceName, target, targetName,m);
        SampleBoth_Estimation(source, sourceName, target, targetName, m);

        System.out.println();
        // sparseSupportWithBloomFilter(source, sourceName, target, targetName,
        // m);
    }

    private static double sparseSupportWithBloomFilter(String source,
                                                       String sourceName, String target, String targetName, double m,
                                                       BloomFilter<String> S) {
        double support = 0;
        String querySource = "select " + sourceName + " from " + source
                + " where random() < " + m;
        ArrayList<String> R = new ArrayList<String>();

        Statement st;
        ResultSet rs;
        try {
            st = Main.connection.createStatement();
            rs = st.executeQuery(querySource);

            while (rs.next()) {
                String next = rs.getString(1);
                if (next != null) {
                    R.add(next);
                }
            }
            rs.close();
            st.close();

            for (String str : R) {
                if (S.contains(str)) {
                    support++;
                }
            }

            support = (1 / m) * support;
            // System.out.println("sparseSupportWithBloomFilter : " + support+
            // ", Sampling : " + m);

            // TODO : get actual count, provide a sanity bound in-case it
            // exceeds count.
            return support;
        } catch (Exception e) {
            e.printStackTrace();
        }

        return -1;
    }

    private static double sparseSupportWithTargetScaling(String source,
                                                         String sourceName, String target, String targetName, double m, int completeDistinctCount) {
        double support = 0;
        ArrayList<String> sample1List = new ArrayList<String>();
        ArrayList<String> sample2List = new ArrayList<String>();

        String querySource = "select " + sourceName + " from " + source
                + " where random() < " + m;
        String queryTarget = "select " + targetName + " from " + target
                + " where random() < " + m;
        Statement st;
        ResultSet rs;
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
                if (sample2List.contains(s)) {
                    support++;
                }
            }
            support = support * (1 / m);
            // Modify to also take target scaling into account

            int sampleDistinctSize = disinctitem.size();
            if (sampleDistinctSize != 0)
                support *= (completeDistinctCount / sampleDistinctSize);
//			System.out.println("sparseSupportWithTargetScaling :" + support
//					+ ", sampling: " + m);

            rs.close();
            st.close();
            return support;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return -1;
    }

    private static double actualSupport(String source, String sourceName,
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
                double val = Double.parseDouble(rs.getString(1));
                // System.out.println("Actual support = " + val);
                return val;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return -1;
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
                + " where random() < " + m;
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
                if (S.contains(str)) {
                    support++;
                }
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
                + " where random() < " + m;
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
                if (R.contains(str)) {
                    support++;
                }
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
                + " where random() < " + m;
        String queryTarget = "select " + targetName + " from " + target
                + " where random() < " + m;
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
                + " where random() < " + m;
        String queryTarget = "select " + targetName + " from " + target
                + " where random() < " + m;
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
                if (sample2List.contains(s)) {
                    support++;
                }
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
            if (sample2List.contains(s)) {
                resultList.add(s);
            }
        }

        return resultList;
    }

    public static void doStep3SourceOnlyWithBloom() {
        String R = Main.table1Name;
        String S = Main.table2Name;
        // Sampling percentage
        double m = Main.sampleValue;
        for (int j = 0; j < Main.table2NoOfAttrs; ++j) {
            String dest = Main.table2AttrNames[j];
            // First prmtr is BF size in terms of bits
            BloomFilter<String> bloom = new FilterBuilder(2 * Main.targetrelSize,
                    Main.fpProbability).buildBloomFilter();
            String queryTarget = "select " + dest + " from " + S;
            Statement st;
            ResultSet rs;
            try {
                st = Main.connection.createStatement();
                rs = st.executeQuery(queryTarget);

                while (rs.next()) {
                    String next = rs.getString(1);
                    if (next != null) {
                        bloom.add(next);
                    }
                }
                rs.close();
                st.close();

            } catch (Exception e) {
                e.printStackTrace();
            }
            for (int i = 0; i < Main.table1NoOfAttrs; ++i) {
                if (Main.dataTypeCompatibilityMatrix[i][j]) {
                    String src = Main.table1AttrNames[i];
                    // have to consider both pairs
                    if (Main.debugMode)
                        System.out.println(src + " vs " + dest);
                    double support = sparseSupportWithBloomFilter(R, src, S,
                            dest, m, bloom);
                    Main.all.add(new Pair(src, dest, support));
                }
            }
            bloom.clear();
        }

        Collections.sort(Main.all);
        if (Main.debugMode) {
            for (Pair cur : Main.all) {
                System.out.println(cur.toString());
            }
        }

        // What threshold value to set - to be studied. 10% is okay?


        System.out.println("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
        System.out.println("Using source sampling + target bloom filter");
        System.out.println("pairs which satisfy support threshold : "
                + Main.threshold);

        for (Pair cur : Main.all) {
            if (cur.support >= Main.threshold) {
                System.out.println(cur.toString());
            }
        }

        System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        Main.all.clear();
    }

    public static void doStep3WithTargetScaling() {
        String R = Main.table1Name;
        String S = Main.table2Name;

        // Sampling percentage
        double m = Main.sampleValue;
        for (int j = 0; j < Main.table2NoOfAttrs; ++j) {
            int completeDistinctCount = 0;
            int sampleDistinctCount = 0;
            Statement st;
            ResultSet rs;
            String dest = Main.table2AttrNames[j];
            // First prmtr is BF size in terms of bits
            BloomFilter<String> bloom = new FilterBuilder(2 * Main.targetrelSize,
                    Main.fpProbability).buildBloomFilter();


            //1. Find distinct count of complete relation using query.

            String queryDistCnt = "select count(distinct " + dest + " ) from " + S;

            try {
                st = Main.connection.createStatement();
                rs = st.executeQuery(queryDistCnt);
                if (rs.next())
                    completeDistinctCount = Integer.parseInt(rs.getString(1));
                rs.close();
                st.close();
            } catch (Exception e) {
                e.printStackTrace();
            }


            //2. Find distinct count of sample relation using Set
            String querySampleDistinct = "select " + dest + " from " + S + " where random() < " + Main.sampleValue;
            Set<String> distinctSet = new HashSet<String>();
            try {
                st = Main.connection.createStatement();
                rs = st.executeQuery(querySampleDistinct);

                while (rs.next()) {
                    String next = rs.getString(1);
                    if (next != null) {
                        bloom.add(next);
                        distinctSet.add(next);
                    }
                }
                rs.close();
                st.close();

            } catch (Exception e) {
                e.printStackTrace();
            }
            sampleDistinctCount = distinctSet.size();
            if (sampleDistinctCount == 0) {
                sampleDistinctCount = 0;
            }
            for (int i = 0; i < Main.table1NoOfAttrs; ++i) {
                if (Main.dataTypeCompatibilityMatrix[i][j]) {

                    String src = Main.table1AttrNames[i];
                    // have to consider both pairs
                    if (Main.debugMode)
                        System.out.println(src + " vs " + dest);
                    double support = sparseSupportWithTargetScalingBloom(R, src, S, dest, m, bloom, sampleDistinctCount, completeDistinctCount);
                    Main.all.add(new Pair(src, dest, support));
                }
            }
        }

        Collections.sort(Main.all);
        if (Main.debugMode) {
            for (Pair cur : Main.all) {
                System.out.println(cur.toString());
            }
        }

        // What threshold value to set - to be studied. 10% is okay?


        System.out.println("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
        System.out.println("Using Target Scaling with Both Sampling + bloom filter");
        System.out.println("pairs which satisfy support threshold : "
                + Main.threshold);

        for (Pair cur : Main.all) {
            if (cur.support >= Main.threshold) {
                System.out.println(cur.toString());
            }
        }

        System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        Main.all.clear();
    }


    public static void doStep3WithTargetScalingWithoutBloom() {
        String R = Main.table1Name;
        String S = Main.table2Name;

        // Sampling percentage
        double m = Main.sampleValue;
        for (int j = 0; j < Main.table2NoOfAttrs; ++j) {
            int completeDistinctCount = 0;
            Statement st;
            ResultSet rs;
            String dest = Main.table2AttrNames[j];

            //1. Find distinct count of complete relation using query.

            String queryDistCnt = "select count(distinct " + dest + " ) from " + S;

            try {
                st = Main.connection.createStatement();
                rs = st.executeQuery(queryDistCnt);
                if (rs.next())
                    completeDistinctCount = Integer.parseInt(rs.getString(1));
                rs.close();
                st.close();
            } catch (Exception e) {
                e.printStackTrace();
            }


            for (int i = 0; i < Main.table1NoOfAttrs; ++i) {
                if (Main.dataTypeCompatibilityMatrix[i][j]) {
                    String src = Main.table1AttrNames[i];
                    // have to consider both pairs
                    double support = sparseSupportWithTargetScaling(R, src, S, dest, m, completeDistinctCount);
                    Main.all.add(new Pair(src, dest, support));
                }
            }
        }

        Collections.sort(Main.all);
        if (Main.debugMode) {
            for (Pair cur : Main.all) {
                System.out.println(cur.toString());
            }
        }

        // What threshold value to set - to be studied. 10% is okay?


        System.out.println("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
        System.out.println("Using Target Scaling with Both Sampling");
        System.out.println("pairs which satisfy support threshold : "
                + Main.threshold);

        for (Pair cur : Main.all) {
            if (cur.support >= Main.threshold) {
                System.out.println(cur.toString());
            }
        }

        System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");

    }


    private static double sparseSupportWithTargetScalingBloom(String source, String sourceName,
                                                              String target, String targetName, double m,
                                                              BloomFilter<String> bloom, int sampleDistinctCount, int completeDistinctCount) {

        double support = 0;

        String querySource = "select " + sourceName + " from " + source
                + " where random() < " + m;

        Statement st;
        ResultSet rs;
        try {
            st = Main.connection.createStatement();
            rs = st.executeQuery(querySource);


            while (rs.next()) {
                String nxt = rs.getString(1);
                if (bloom.contains(nxt))
                    support++;
            }
            rs.close();

            support = support * (1 / m);
            // Modify to also take target scaling into account

            if (sampleDistinctCount != 0)
                support *= (completeDistinctCount / sampleDistinctCount);

//			System.out.println("sparseSupportWithTargetScalingWithBloom :" + support
//					+ ", sampling: " + m);

            rs.close();
            st.close();

            return support;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return -1;
    }

    public static void doStep3WithActualSupport() {
        String R = Main.table1Name;
        String S = Main.table2Name;
        // Sampling percentage
        List<Pair> all = new ArrayList<Pair>();
        double m = Main.sampleValue;
        for (int i = 0; i < Main.table1NoOfAttrs; ++i) {
            for (int j = 0; j < Main.table2NoOfAttrs; ++j) {
                if (Main.dataTypeCompatibilityMatrix[i][j]) {
                    String src = Main.table1AttrNames[i];
                    String dest = Main.table2AttrNames[j];
                    // have to consider both pairs
                    if (Main.debugMode)
                        System.out.println(src + " vs " + dest);
                    double support = actualSupport(R, src, S, dest, m);
                    all.add(new Pair(src, dest, support));
                }
            }
        }

        Collections.sort(all);

        if (Main.debugMode) {
            for (Pair cur : all) {
                System.out.println(cur.toString());
            }
        }


        // What threshold value to set - to be studied. 10% is okay?


        System.out.println("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
        System.out.println("Using actual support method");
        System.out.println("pairs which satisfy support threshold : "
                + Main.threshold);

        for (Pair cur : all) {
            if (cur.support >= Main.threshold) {
                System.out.println(cur.toString());
            }
        }
        System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");

    }


    public static void doStep3WithActualSupportGeneric() {
        String sourceRelName = Main.table1Name;
        String targetRelName = Main.table2Name;


        int itemsetNumber = 0;
        //while not complete
        do {
            //increase the itemset that is being looked at
            itemsetNumber++;

            //generate the candidates
            generateCandidates(itemsetNumber);
            System.out.println("before pruning");
            for (ItemSet cur : candidateItemSetList) {
                System.out.println(cur.toString());
            }
            //determine and display frequent itemsets
            calculateFrequentItemsets();
            System.out.println("after pruning");
            if (candidateItemSetList.size() != 0) {
                for (ItemSet cur : candidateItemSetList) {
                    System.out.println(cur.toString());
                }
            }
            //if there are <=1 frequent items, then its the end. This prevents reading through the database again. When there is only one frequent itemset.
        } while (candidateItemSetList.size() > 1);


    }


    private static void calculateFrequentItemsets() {
        double support;
        ArrayList<ItemSet> frequentItemSetList = new ArrayList<ItemSet>();
        for (int i = 0; i < candidateItemSetList.size(); i++) {
            ItemSet is = candidateItemSetList.get(i);
            support = getActualSupportGeneric(Main.table1Name, Main.table2Name,
                    is.sourceList, is.targetList);
            if (support > Main.threshold) {
                frequentItemSetList.add(is);
                globalFreqItemSetList.add(is);
            }
        }
        candidateItemSetList.clear();
        candidateItemSetList.addAll(frequentItemSetList);
    }

    private static void generateCandidates(int itemsetNumber) {
        ArrayList<ItemSet> tempCandidatesList = new ArrayList<ItemSet>();
        if (itemsetNumber == 1) {
            for (int i = 0; i < Main.table1NoOfAttrs; ++i) {
                for (int j = 0; j < Main.table2NoOfAttrs; ++j) {
                    if (Main.dataTypeCompatibilityMatrix[i][j]) {
                        // initialize a new itemset
                        ItemSet is = new ItemSet();
                        is.sourceList = new ArrayList<Integer>();
                        is.sourceList.add(i);

                        is.targetList = new ArrayList<Integer>();
                        is.targetList.add(j);

                        is.frequent = false;
                        is.support = 0;

                        // have to consider both pairs
                        if (Main.debugMode)
                            System.out.println(is.sourceList.toString() + " vs " +
                                    is.targetList.toString());

                        //actualSupport(sourceRelName, src, targetRelName, dest, m);
                        double support = getActualSupportGeneric(Main.table1Name, Main.table2Name,
                                is.sourceList, is.targetList);

                        is.support = support;
                        tempCandidatesList.add(is);
                    }
                }
            }
        } else {
            for (int i = 0; i < candidateItemSetList.size() - 1; i++) {
                for (int j = i + 1; j < candidateItemSetList.size(); j++) {
                    ItemSet candidate1 = candidateItemSetList.get(i);
                    ItemSet candidate2 = candidateItemSetList.get(j);

                    //create the strigns
                    String str1 = new String();
                    String str2 = new String();

                    String targetStr = new String();

                    //make a string of the first n-2 tokens of the strings
                    int s = 0;
                    for (s = 0; s < itemsetNumber - 2; s++) {
                        str1 = str1 + " " + candidate1.sourceList.get(s);
                        str2 = str2 + " " + candidate2.sourceList.get(s);

                        targetStr = targetStr + " " + candidate1.targetList.get(s);
                    }

                    //if they have the same n-2 tokens, add them together
                    if (str2.compareToIgnoreCase(str1) == 0) {
                        str1 += " " + candidate1.sourceList.get(s);
                        str1 += " " + candidate2.sourceList.get(s);

                        targetStr += " " + candidate1.targetList.get(s);
                        targetStr += " " + candidate2.targetList.get(s);

                        ItemSet is = new ItemSet();
                        is.sourceList = new ArrayList<Integer>();
                        StringTokenizer st = new StringTokenizer(str1);
                        while (st.hasMoreTokens()) {
                            is.sourceList.add(Integer.parseInt(st.nextToken()));
                        }

                        is.targetList = new ArrayList<Integer>();
                        st = new StringTokenizer(targetStr);
                        while (st.hasMoreTokens()) {
                            is.targetList.add(Integer.parseInt(st.nextToken()));
                        }

                        is.frequent = false;
                        is.support = 0;

                        // have to consider both pairs
                        if (Main.debugMode) {
                            System.out.println(is.sourceList.toString() + " vs "
                                    + is.targetList.toString());
                        }
                        tempCandidatesList.add(is);
                    }
                }
            }
        }

        candidateItemSetList.clear();
        candidateItemSetList.addAll(tempCandidatesList);
    }

    public static String gN(int tableId, int i) {
        return tableId==0 ? Main.table1AttrNames[i] :Main.table2AttrNames[i];
    }
    private static double getActualSupportGeneric(String r, String s,
         ArrayList<Integer> r_attr, ArrayList<Integer> s_attr) {

        String srcSelectStr = "";
        for(Integer i : r_attr) {
           srcSelectStr += r + "." + gN(0,i) + " , ";
        }
        srcSelectStr = srcSelectStr.substring(0, srcSelectStr.length() - 3);

        String tgtSelectStr = "";
        for(Integer i : s_attr) {
            tgtSelectStr += gN(1,i) + " , ";
        }
        tgtSelectStr = tgtSelectStr.substring(0, tgtSelectStr.length() - 3);

        String strCompare = "";
        for (int i = 0; i < r_attr.size(); i++) {

            strCompare += r + "." + gN(0,r_attr.get(i)) +  " = " + "table2." + gN(1,s_attr.get(i)) + " and ";
        }
        strCompare = strCompare.substring(0, strCompare.length() - 5);

        String querySource = "select count(*) from (select " + srcSelectStr + " from " + r + " inner join " +
                "((select distinct " + tgtSelectStr + " from " + s + ")) table2  on " +
                "" + strCompare + ") as djoin;";

        Statement st;
        ResultSet rs;

        try {
            st = Main.connection.createStatement();
            rs = st.executeQuery(querySource);

            if (rs.next()) {
                String ans = rs.getString(1);
                //System.out.println("Actual support = " + ans);
                return Double.parseDouble(ans);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return -1.0;
    }
}

class ItemSet {
    ArrayList<Integer> sourceList;
    ArrayList<Integer> targetList;
    double support;
    boolean frequent;

    public String toString() {
        return sourceList.toString() + " , " + targetList.toString() + " ,Support = " + support;
    }
}