package com.dimas.brosalin.streaming.json;

import com.dimas.brosalin.streaming.json.metamodel.MetaTable;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Created by DmitriyBrosalin on 10/05/2020.
 */
public class JsonRelation {

    private final SQLContext sqlContext;
    private final Map<String, String> parameters;
    private static final ObjectMapper objectMapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    private static Logger log = LoggerFactory.getLogger(JsonRelation.class);

    public JsonRelation(SQLContext sqlContext, Map<String, String> parameters){
        this.sqlContext = sqlContext;
        this.parameters = parameters;
    }

    public StructType createStructType(){
        StructType structType = new StructType();
        return structType;
    }

    private List<StructField> createStructFieldList(){
        Optional<MetaTable> metaTable = JsonRelationService.metaTableProvider(Optional.ofNullable(parameters.get("schema")));
        List<StructField> fieldList = metaTable.map(JsonRelationService::structFieldListProvider)
                .orElseGet(() -> JsonRelationService.structFieldListProvider(Optional.ofNullable(parameters.get("path"))));
        return fieldList;
    }



}
