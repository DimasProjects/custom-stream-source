package com.dimas.brosalin.streaming;

import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.sources.RelationProvider;
import org.apache.spark.sql.sources.SchemaRelationProvider;
import org.apache.spark.sql.types.StructType;
import scala.collection.immutable.Map;

import java.util.Optional;

/**
 * Created by DmitriyBrosalin on 10/05/2020.
 */
public class CustomStreamingSource implements RelationProvider, SchemaRelationProvider, DataSourceRegister {

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
}
