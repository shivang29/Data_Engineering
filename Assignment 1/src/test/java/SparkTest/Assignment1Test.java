package SparkTest;
import Spark.AggregrateSummary;
import Spark.Assignment1;
import Spark.ReturnsData;
import Spark.TotalSales;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.exp;
import static org.junit.Assert.assertArrayEquals;

import static org.apache.spark.sql.functions.col;
import static org.junit.Assert.assertEquals;
public class Assignment1Test {

    private static SparkSession Spark;

    @BeforeClass
    public static void setup() {
        Spark = SparkSession.builder()
                .appName("Assignment1Test")
                .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
                .master("local[*]")
                .getOrCreate();
    }

    @AfterClass
    public static void tearDown() {
        if (Spark != null) {
            Spark.stop();
        }
    }

    @Test
    public void testFinalDataSetComparisonWithResult(){
        //TEST DATAset FOR sales
        List<TotalSales> totalSalesList = new ArrayList<>();
        totalSalesList.add(new TotalSales("CA-2014-AB10015140-41954", "11-11-2014", "First Class", "Consumer", "Oklahoma City", "Oklahoma", "United States", "Technology", "Phones", 221.98, 2, 0, "-$62.15", 40.77, "No"));
        totalSalesList.add(new TotalSales("IN-2014-JR162107-41675", "02-05-2014", "Second Class", "Corporate", "Wollongong", "New South Wales", "Australia", "Furniture", "Chairs", 3709.40, 9, 0.1, "-$288.77", 923.63, "No"));
        totalSalesList.add(new TotalSales("IN-2014-CR127307-41929", "10/17/2014", "First Class", "Consumer", "Brisbane", "Queensland", "Australia", "Technology", "Phones", 5175.17, 9, 0.1, "$919.97", 915.49, "No"));
        totalSalesList.add(new TotalSales("ES-2014-KM1637548-41667", "1/28/2014", "First Class", "Home Office", "Berlin", "Berlin", "Germany", "Technology", "Phones", 2892.51, 5, 0.1, "-$96.54", 910.16, "No"));
        totalSalesList.add(new TotalSales("SG-2014-RH9495111-41948", "11-05-2014", "Same Day", "Consumer", "Dakar", "Dakar", "Senegal", "Technology", "Phones", 2832.96, 8, 0, "$311.52", 903.04, "No"));
        totalSalesList.add(new TotalSales("IN-2014-JM156557-41818", "6/28/2014", "Second Class", "Corporate", "Sydney", "New South Wales", "Australia", "Technology", "Phones", 2862.68, 5, 0.1, "$763.28", 897.35, "No"));
        totalSalesList.add(new TotalSales("IN-2012-TS2134092-41219", "11-06-2012", "First Class", "Consumer", "Porirua", "Wellington", "New Zealand", "Furniture", "Phones", 1822.08, 4, 0, "$564.84", 894.77, "No"));
        totalSalesList.add(new TotalSales("IN-2012-PB19210127-41259", "12/16/2012", "First Class", "Corporate", "Taipei", "Taipei City", "Taiwan", "Furniture", "Tables", 1715.16, 2, 0, "$720.36", 725.57, "Yes"));
        totalSalesList.add(new TotalSales("IN-2012-PB19210127-41259", "12/16/2012", "First Class", "Corporate", "Taipei", "Taipei City", "Taiwan", "Furniture", "Bookcases", 2197.50, 5, 0, "$153.75", 627.27, "Yes"));
        totalSalesList.add(new TotalSales("IN-2015-JH158207-42140", "5/16/2015", "Second Class", "Consumer", "Melbourne", "Victoria", "Australia", "Technology", "Phones", 2863.35, 5, 0.1, "$858.90", 581.88, "Yes"));

        Dataset<Row> totalSalesDataset =  Spark.createDataset(totalSalesList, Encoders.bean(TotalSales.class)).toDF();
        //change column name from pojo
        totalSalesDataset = totalSalesDataset.withColumnRenamed("orderDate", "Order Date");
        totalSalesDataset = totalSalesDataset.withColumnRenamed("orderID", "Order ID");
        totalSalesDataset = totalSalesDataset.withColumnRenamed("profit", "Profit");
        totalSalesDataset = totalSalesDataset.withColumnRenamed("category", "Category");
        totalSalesDataset = totalSalesDataset.withColumnRenamed("subCategory", "Sub-Category");
        totalSalesDataset = totalSalesDataset.withColumnRenamed("quantity", "Quantity");

        // Convert "Order Date" column to a uniform date format
        totalSalesDataset = totalSalesDataset.withColumn("Order Date",
                functions.when(
                        functions.to_date(functions.col("Order Date"), "MM-dd-yyyy").isNotNull(),
                        functions.date_format(functions.to_date(functions.col("Order Date"), "MM-dd-yyyy"), "MM/dd/yyyy")
                ).when(
                        functions.to_date(functions.col("Order Date"), "MM/dd/yyyy").isNotNull(),
                        functions.date_format(functions.to_date(functions.col("Order Date"), "MM/dd/yyyy"), "MM/dd/yyyy")
                ).otherwise(functions.col("Order Date"))
        );
        //sample list for returns data.
        List<ReturnsData> returnsDataList = new ArrayList<>();
        returnsDataList.add(new ReturnsData("CA-2012-SA20830140-41210", "Yes"));
        returnsDataList.add(new ReturnsData("IN-2012-PB19210127-41259", "Yes"));
        returnsDataList.add(new ReturnsData("IN-2015-JH158207-42140", "Yes"));

        Dataset<Row> testReturnsdataset = Spark.createDataset(returnsDataList, Encoders.bean(ReturnsData.class)).toDF();
        testReturnsdataset = testReturnsdataset.withColumnRenamed("orderID", "Order ID");
        testReturnsdataset = testReturnsdataset.withColumnRenamed("returned", "Returned");

        Dataset<Row> processedData = Assignment1.processSalesAndReturnsData(totalSalesDataset, testReturnsdataset);


        List<AggregrateSummary> summaryList = new ArrayList<>();
        summaryList.add(new AggregrateSummary("2014", 11, "Technology", "Phones", 10, "$249.37"));
        summaryList.add(new AggregrateSummary("2014", 2, "Furniture", "Chairs", 9, "-$288.77"));
        summaryList.add(new AggregrateSummary("2014", 10, "Technology", "Phones", 9, "$919.97"));
        summaryList.add(new AggregrateSummary("2014", 1, "Technology", "Phones", 5, "-$96.54"));
        summaryList.add(new AggregrateSummary("2014", 6, "Technology", "Phones", 5, "$763.28"));
        summaryList.add(new AggregrateSummary("2012", 11, "Furniture", "Phones", 4, "$564.84"));

        //get the expected dataset
        Dataset<Row> expectedData = Spark.createDataset(summaryList,Encoders.bean(AggregrateSummary.class)).toDF();
        expectedData = expectedData.withColumnRenamed("SubCategory", "Sub-Category");

        expectedData = expectedData.withColumn("Month",col("Month").cast(DataTypes.IntegerType))
                .withColumn("TotalQuantitySold",col("TotalQuantitySold").cast(DataTypes.IntegerType));
        expectedData = expectedData.select("Year","Month","Category","Sub-Category","TotalQuantitySold","TotalProfit");
        Row[] processedDataInRow = (Row[]) processedData.collect();
        Row[] expectedDataInRow = (Row[]) expectedData.collect();
        assertArrayEquals(expectedDataInRow,processedDataInRow);
    }

    //Test if count is matching
    @Test
    public void testSalesDataProcessingCount() {

        //TEST DATAset FOR sales
        List<TotalSales> totalSalesList = new ArrayList<>();
        totalSalesList.add(new TotalSales("CA-2014-AB10015140-41954", "11-11-2014", "First Class", "Consumer", "Oklahoma City", "Oklahoma", "United States", "Technology", "Phones", 221.98, 2, 0, "-$62.15", 40.77, "No"));
        totalSalesList.add(new TotalSales("IN-2014-JR162107-41675", "02-05-2014", "Second Class", "Corporate", "Wollongong", "New South Wales", "Australia", "Furniture", "Chairs", 3709.40, 9, 0.1, "-$288.77", 923.63, "No"));
        totalSalesList.add(new TotalSales("IN-2014-CR127307-41929", "10/17/2014", "First Class", "Consumer", "Brisbane", "Queensland", "Australia", "Technology", "Phones", 5175.17, 9, 0.1, "$919.97", 915.49, "No"));
        totalSalesList.add(new TotalSales("ES-2014-KM1637548-41667", "1/28/2014", "First Class", "Home Office", "Berlin", "Berlin", "Germany", "Technology", "Phones", 2892.51, 5, 0.1, "-$96.54", 910.16, "No"));
        totalSalesList.add(new TotalSales("SG-2014-RH9495111-41948", "11-05-2014", "Same Day", "Consumer", "Dakar", "Dakar", "Senegal", "Technology", "Phones", 2832.96, 8, 0, "$311.52", 903.04, "No"));
        totalSalesList.add(new TotalSales("IN-2014-JM156557-41818", "6/28/2014", "Second Class", "Corporate", "Sydney", "New South Wales", "Australia", "Technology", "Phones", 2862.68, 5, 0.1, "$763.28", 897.35, "No"));
        totalSalesList.add(new TotalSales("IN-2012-TS2134092-41219", "11-06-2012", "First Class", "Consumer", "Porirua", "Wellington", "New Zealand", "Furniture", "Phones", 1822.08, 4, 0, "$564.84", 894.77, "No"));
        totalSalesList.add(new TotalSales("IN-2012-PB19210127-41259", "12/16/2012", "First Class", "Corporate", "Taipei", "Taipei City", "Taiwan", "Furniture", "Tables", 1715.16, 2, 0, "$720.36", 725.57, "Yes"));
        totalSalesList.add(new TotalSales("IN-2012-PB19210127-41259", "12/16/2012", "First Class", "Corporate", "Taipei", "Taipei City", "Taiwan", "Furniture", "Bookcases", 2197.50, 5, 0, "$153.75", 627.27, "Yes"));
        totalSalesList.add(new TotalSales("IN-2015-JH158207-42140", "5/16/2015", "Second Class", "Consumer", "Melbourne", "Victoria", "Australia", "Technology", "Phones", 2863.35, 5, 0.1, "$858.90", 581.88, "Yes"));

        Dataset<Row> totalSalesDataset =  Spark.createDataset(totalSalesList, Encoders.bean(TotalSales.class)).toDF();
        //change column name from pojo
        totalSalesDataset = totalSalesDataset.withColumnRenamed("orderDate", "Order Date");
        totalSalesDataset = totalSalesDataset.withColumnRenamed("orderID", "Order ID");
        totalSalesDataset = totalSalesDataset.withColumnRenamed("profit", "Profit");
        totalSalesDataset = totalSalesDataset.withColumnRenamed("category", "Category");
        totalSalesDataset = totalSalesDataset.withColumnRenamed("subCategory", "Sub-Category");
        totalSalesDataset = totalSalesDataset.withColumnRenamed("quantity", "Quantity");

        totalSalesDataset = totalSalesDataset.withColumn("Order Date",
                functions.when(
                        functions.to_date(functions.col("Order Date"), "MM-dd-yyyy").isNotNull(),
                        functions.date_format(functions.to_date(functions.col("Order Date"), "MM-dd-yyyy"), "MM/dd/yyyy")
                ).when(
                        functions.to_date(functions.col("Order Date"), "MM/dd/yyyy").isNotNull(),
                        functions.date_format(functions.to_date(functions.col("Order Date"), "MM/dd/yyyy"), "MM/dd/yyyy")
                ).otherwise(functions.col("Order Date"))
        );
        //sample list for returns data.
        List<ReturnsData> returnsDataList = new ArrayList<>();
        returnsDataList.add(new ReturnsData("CA-2012-SA20830140-41210", "Yes"));
        returnsDataList.add(new ReturnsData("IN-2012-PB19210127-41259", "Yes"));
        returnsDataList.add(new ReturnsData("IN-2015-JH158207-42140", "Yes"));

        Dataset<Row> testReturnsdataset = Spark.createDataset(returnsDataList, Encoders.bean(ReturnsData.class)).toDF();
        testReturnsdataset = testReturnsdataset.withColumnRenamed("orderID", "Order ID");
        testReturnsdataset = testReturnsdataset.withColumnRenamed("returned", "Returned");
       Dataset<Row> processedData = Assignment1.processSalesAndReturnsData(totalSalesDataset, testReturnsdataset);

        // as count it should contains 6 rows.
        long expectedRowCount = 6;
        assertEquals(expectedRowCount, processedData.count());

        // Add more assertions based on your processing logic and expected results
    }


}
