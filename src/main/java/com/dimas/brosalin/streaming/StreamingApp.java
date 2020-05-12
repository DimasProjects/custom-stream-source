package com.dimas.brosalin.streaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQueryException;

public class StreamingApp {

    public static void main(String[] args) throws StreamingQueryException {
        String pathToMetaFile = args[0];
        String pathToTableFile = args[1];

        SparkSession sparkSession = SparkSession.builder().getOrCreate();

        Dataset<Row> reader = sparkSession.readStream()
                .format("com.dimas.brosalin.streaming.CustomStreamingSource")
                .option("path", pathToTableFile)
                .option("schema", pathToMetaFile)
                .load();

        reader.filter(functions.col("name").isin("google", "apple", "gucli", "pulya"))
                .groupBy(functions.col("number"))
                .count()
                .writeStream()
                .format("console")
                .outputMode(OutputMode.Complete())
                .start()
                .awaitTermination();
    }

}
