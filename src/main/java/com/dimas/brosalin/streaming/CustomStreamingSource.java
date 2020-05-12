package com.dimas.brosalin.streaming;

import com.dimas.brosalin.streaming.json.JsonSource;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.execution.streaming.Source;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.sources.StreamSourceProvider;
import org.apache.spark.sql.types.StructType;
import scala.Option;
import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.collection.immutable.Map;

/**
 * Created by DmitriyBrosalin on 10/05/2020.
 */
public class CustomStreamingSource implements StreamSourceProvider, DataSourceRegister{

    @Override
    public String shortName() {
        return "custom-streaming-source";
    }

    @Override
    public Tuple2<String, StructType> sourceSchema(SQLContext sqlContext, Option<StructType> schema,
                                                   String providerName, Map<String, String> parameters) {
        System.out.println("\n\nPARAMETERS: " + parameters);
        System.out.println("\n\nPARAMETERS: " + JavaConversions.mapAsJavaMap(parameters));
        return new Tuple2<>(shortName(), JsonSource.createStructType(JavaConversions.mapAsJavaMap(parameters)));
    }

    @Override
    public Source createSource(SQLContext sqlContext, String metadataPath, Option<StructType> schema,
                               String providerName, Map<String, String> parameters) {
        System.out.println("\n\nPARAMETERS 2: " + parameters);
        System.out.println("\n\nPARAMETERS 2: " + JavaConversions.mapAsJavaMap(parameters));
        return new JsonSource(sqlContext, JavaConversions.mapAsJavaMap(parameters));
    }
}
