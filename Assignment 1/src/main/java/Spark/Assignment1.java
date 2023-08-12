package Spark;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import static org.apache.spark.sql.functions.*;

public class Assignment1 {
//    private static final String SALES_INPUT_PATH = ".\\Dataset\\Global Superstore Sales - Global Superstore Sales.csv";
//    private static final String RETURNS_INPUT_PATH = ".\\Dataset\\Global Superstore Sales - Global Superstore Returns.csv";
    public static Dataset<Row> processSalesAndReturnsData(Dataset<Row> salesds,Dataset<Row> returnsds){
        salesds.printSchema();
        salesds = salesds.withColumn("Array_Date", functions.split(col("Order Date"),"/"));

        salesds = salesds.withColumn("Month", functions.expr("Array_Date[0]"))
                .withColumn("Date", functions.expr("Array_Date[1]"))
                .withColumn("Year", functions.expr("Array_Date[2]"))
                .drop(col("Array_Date"));
        Dataset<Row> combinedds = salesds.join(broadcast(returnsds), salesds.col("Order ID").equalTo(returnsds.col("Order ID")), "leftouter")
                .drop(returnsds.col("ORDER ID"))
                .filter((col("Returned").isNull()))
                .drop(returnsds.col("Returned"));

        Dataset<Row> outputds = combinedds.withColumn("NumericProfit",
                        functions.when(functions.col("Profit").startsWith("-"),
                                functions.expr("CAST(SUBSTRING(Profit, 3) AS DOUBLE) * -1")
                        ).otherwise(
                                functions.expr("CAST(SUBSTRING(Profit, 2) AS DOUBLE)")
                        ))
                        .groupBy("Year", "Month", "Category", "Sub-Category")
                        .agg(functions.sum("Quantity").alias("TotalQuantitySold"),
                            functions.sum("NumericProfit").alias("TotalProfit"))
                        .withColumn("TotalProfit", functions.when(functions.col("TotalProfit").lt(0),
                                functions.concat(lit("-$"), functions.format_number(functions.col("TotalProfit").multiply(-1), 2)))
                        .otherwise(functions.concat(lit("$"), functions.format_number(col("TotalProfit"), 2))))
                        .withColumn("TotalQuantitySold",functions.col("TotalQuantitySold").cast(DataTypes.IntegerType))
                        .withColumn("Month",functions.col("Month").cast(DataTypes.IntegerType));

        return outputds;
    }

    public static void main(String[] args) throws IOException {
//        if (args.length != 2) {
//            System.err.println("Usage: Assignment1 <salesInputPath> <returnsInputPath>");
//            System.exit(1);
//        }
        SparkSession spark = SparkSession
                .builder()
                .appName("Assignment1")
                .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
                .master("local[*]")
                .getOrCreate();
        String SALES_INPUT_PATH = args[0];
        String RETURNS_INPUT_PATH = args[1];
        Dataset<Row> salesds = spark.read().option("header","true").csv(SALES_INPUT_PATH);
        //use to consistent of order date format as MM/dd/YYYY.
        salesds = salesds.withColumn("Order Date",
                functions.when(
                        functions.to_date(functions.col("Order Date"), "MM-dd-yyyy").isNotNull(),
                        functions.date_format(functions.to_date(functions.col("Order Date"), "MM-dd-yyyy"), "MM/dd/yyyy")
                ).when(
                        functions.to_date(functions.col("Order Date"), "MM/dd/yyyy").isNotNull(),
                        functions.date_format(functions.to_date(functions.col("Order Date"), "MM/dd/yyyy"), "MM/dd/yyyy")
                ).otherwise(functions.col("Order Date"))
        );
       // salesds.show();

        Dataset<Row> returnsds = spark.read().option("header", "true").csv(RETURNS_INPUT_PATH);

        Dataset<Row> finalResult = processSalesAndReturnsData(salesds,returnsds);
     //  finalResult.show();
        finalResult.write()
                .format("csv")
                .option("header", "true")
                .partitionBy("Year","Month")
                .save(".\\Dataset\\report\\combinedds");
        spark.stop();
    }
}