package com.dimas.brosalin.streaming;

import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.execution.streaming.Source;
import org.apache.spark.sql.sources.*;
import org.apache.spark.sql.types.StructType;
import scala.Option;
import scala.Tuple2;
import scala.collection.immutable.Map;

import java.util.Optional;

/**
 * Created by DmitriyBrosalin on 10/05/2020.
 */
public class CustomStreamingSource implements RelationProvider, SchemaRelationProvider, StreamSourceProvider, DataSourceRegister {

    @Override
    public BaseRelation createRelation(SQLContext sqlContext, Map<String, String> parameters) {
        Optional.ofNullable(parameters.get("path"))
                .orElseThrow(() -> new RuntimeException("Variable path must be specified!"));
        if(Optional.of(parameters.get("schema")).isPresent()){
            StructType structType = new StructType(); // get scheme
            return createRelation(sqlContext, parameters, structType);
        }else return createRelation(sqlContext, parameters, null);
    }

    @Override
    public String shortName() {
        return "custom-stream";
    }

    @Override
    public BaseRelation createRelation(SQLContext sqlContext, Map<String, String> parameters, StructType schema) {
        return null;
    }

    @Override
    public Tuple2<String, StructType> sourceSchema(SQLContext sqlContext, Option<StructType> schema, String providerName, Map<String, String> parameters) {
        return null;
    }

    @Override
    public Source createSource(SQLContext sqlContext, String metadataPath, Option<StructType> schema, String providerName, Map<String, String> parameters) {
        return null;
    }
}
