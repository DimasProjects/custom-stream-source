package com.dimas.brosalin.streaming.json;

import com.dimas.brosalin.streaming.json.metamodel.MetaColumn;
import com.dimas.brosalin.streaming.json.metamodel.MetaTable;
import com.dimas.brosalin.streaming.json.metamodel.MetaType;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Created by DmitriyBrosalin on 10/05/2020.
 */
public class JsonRelationService {

    private static Logger log = LoggerFactory.getLogger(JsonRelationService.class);
    private static final ObjectMapper objectMapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    private static final String UNRECOGNIZED_DATA_TYPE = "Unrecognized Data Type.";

    public static Optional<MetaTable> metaTableProvider(Optional<String> pathToSchema){
        MetaTable metaTable = null;
        if(pathToSchema.isPresent()) {
            try {
                metaTable = objectMapper.readValue(new File(pathToSchema.get()), MetaTable.class);
            } catch (IOException ex) {
                log.info("Exception has been occured: {}", ex.getMessage());
            }
        }
        return Optional.ofNullable(metaTable);
    }

    public static List<StructField> structFieldListProvider(MetaTable metaTable){
        List<StructField> structFieldList = new ArrayList<>();
        for(MetaColumn column: metaTable.getMetaColumnList()){
            int curIndx = metaTable.getMetaColumnList().indexOf(column);
            DataType curType = structTypeProvider(column.getType())
                    .orElseThrow(() -> new RuntimeException(String.format("%s: %s", UNRECOGNIZED_DATA_TYPE, column.getType())));
            structFieldList.add(curIndx, structFieldProvider(column.getName(), curType));
        }
        return structFieldList;
    }

    public static List<StructField> structFieldListProvider(Optional<String> pathToData){
        List<StructField> structFieldList = new ArrayList<>();

        return structFieldList;
    }

    public static StructField structFieldProvider(String colName, DataType type){
        return new StructField(colName, type, true, Metadata.empty());
    }

    public static Optional<DataType> structTypeProvider(MetaType type){
        DataType curStructType = null;
        if(type.getName().equalsIgnoreCase("DecimalType")){
            curStructType = DataTypes.createDecimalType(type.getPrecision(), type.getScale());
        }else if(type.getName().equalsIgnoreCase("StringType")){
            curStructType = DataTypes.StringType;
        }else if(type.getName().equalsIgnoreCase("BooleanType")){
            curStructType = DataTypes.BooleanType;
        }else if(type.getName().equalsIgnoreCase("TimestampType")){
            curStructType = DataTypes.TimestampType;
        }
        return Optional.ofNullable(curStructType);
    }

}
