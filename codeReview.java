public class KAnonymity {
    /**
     * @param sparksession         spark session
     * @param k_anonymity_constant k constant
     * @param qidAtts              list of Quasi-ID attributes
     * @param initDataset          the dataset to k-Anonymize
     * @return k-Anonymized Datasets
     */
    public static Dataset<Row> kAnonymizeBySuppression(SparkSession sparksession, Dataset<Row> initDataset, List<String> qidAtts, Integer k_anonymity_constant) {

        Dataset<Row> anonymizedDF = sparksession.emptyDataFrame();

        Dataset<Row> tmpDF = sparksession.emptyDataFrame();
        List<Column> groupByQidAttributes = qidAtts.stream().map(functions::col).collect(Collectors.toList());

        // groupBy and count each occurence.
        Dataset<Row> groupedRowsDF = initDataset.withColumn("qidsFreqs", count("*").over(Window.partitionBy(groupByQidAttributes.toArray(new Column[groupByQidAttributes.size()]))));
        Dataset<Row> rowsDeleteDF = groupedRowsDF.select(col("*")).where("qidsFreqs <" + k_anonymity_constant).toDF();
        tmpDF = groupedRowsDF.select(col("*")).where("qidsFreqs >=" + k_anonymity_constant).toDF();


        for (String qidAtt : qidAtts) {
            Dataset<Row> groupedRowsProcDF = rowsDeleteDF.withColumn("attFreq", approx_count_distinct(qidAtt).over(Window.partitionBy(groupByQidAttributes.toArray(new Column[groupByQidAttributes.size()]))));

            Dataset<Row> rowsDeleteDFUpdate = groupedRowsProcDF.select(col("*")).where("attFreq <" + k_anonymity_constant).toDF();

            if (anonymizedDF.count() == 0)
                anonymizedDF = rowsDeleteDFUpdate;
            if (rowsDeleteDF.count() != 0) {
                anonymizedDF = anonymizedDF.drop("attFreq").withColumn(qidAtt, lit("*"));


            }
        }


        return tmpDF.drop("qidsFreqs").union(anonymizedDF.drop("qidsFreqs"));
    }

}