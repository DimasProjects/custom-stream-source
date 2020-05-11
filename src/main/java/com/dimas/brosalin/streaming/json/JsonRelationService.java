package com.dimas.brosalin.streaming.json;

import com.dimas.brosalin.streaming.json.metamodel.MetaColumn;
import com.dimas.brosalin.streaming.json.metamodel.MetaTable;
import com.dimas.brosalin.streaming.json.metamodel.MetaType;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Timestamp;
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
                log.info("Exception has been occured while reading meta: {}", ex.getMessage());
            }
        }
        return Optional.ofNullable(metaTable);
    }

    public static Optional<JsonNode> tableContentProvider(Optional<String> pathToTable){
        JsonNode jsonNode = null;
        if(pathToTable.isPresent()){
            try{
                jsonNode = objectMapper.readTree(new File(pathToTable.get()));
            }catch (IOException ex){
                log.info("Exception has been oocured while reading table content: {}", ex.getMessage());
            }
        }
        return Optional.ofNullable(jsonNode);
    }

    public static List<StructField> structFieldListProvider(MetaTable metaTable){
        List<StructField> structFieldList = new ArrayList<>();
        for(MetaColumn column: metaTable.getMetaColumnList()){
            int curIndx = metaTable.getMetaColumnList().indexOf(column);
            DataType curType = structTypeProvider(column.getType())
                    .orElseThrow(() -> new RuntimeException(String.format("%s: %s", UNRECOGNIZED_DATA_TYPE, column.getType())));
            structFieldList.add(curIndx, structFieldProvider(column.getName(), curType));
        }
        log.info("The result of ");
        return structFieldList;
    }

    //TODO: implement logic when schema was unprovided. In that case set all DataTypes to StringType
    public static List<StructField> structFieldListProvider(Optional<String> pathToData){
        List<StructField> structFieldList = new ArrayList<>();

        return structFieldList;
    }

    public static StructField structFieldProvider(String colName, DataType type){
        return new StructField(colName, type, true, Metadata.empty());
    }

    public static Optional<DataType> structTypeProvider(MetaType type){
        DataType curStructType = null;
        if(type.getType().equalsIgnoreCase("DecimalType")){
            curStructType = DataTypes.createDecimalType(type.getPrecision(), type.getScale());
        }else if(type.getType().equalsIgnoreCase("StringType")){
            curStructType = DataTypes.StringType;
        }else if(type.getType().equalsIgnoreCase("BooleanType")){
            curStructType = DataTypes.BooleanType;
        }else if(type.getType().equalsIgnoreCase("TimestampType")){
            curStructType = DataTypes.TimestampType;
        }
        return Optional.ofNullable(curStructType);
    }

    public static Row rowProvider(JsonNode jsonNode, List<StructField> structFieldList){
        Object[] objects = new Object[structFieldList.size()];
        structFieldList.forEach(field -> {
            int indx = structFieldList.indexOf(field);
            if(field.dataType() instanceof DecimalType) {
                objects[indx] = new BigDecimal(jsonNode.get(field.name()).asText());
            }else if(field.dataType() instanceof TimestampType){
                objects[indx] = Timestamp.valueOf(jsonNode.get(field.name()).asText());
            }else if(field.dataType() instanceof BooleanType){
                objects[indx] = Boolean.valueOf(jsonNode.get(field.name()).asText());
            }else if(field.dataType() instanceof FloatType){
                objects[indx] = Float.valueOf(jsonNode.get(field.name()).asText());
            }else if(field.dataType() instanceof LongType){
                objects[indx] = Long.valueOf(jsonNode.get(field.name()).asText());
            }else if(field.dataType() instanceof StringType){
                objects[indx] = jsonNode.get(field.name()).asText();
            }
        });
        return RowFactory.create(objects);
    }

}
