import com.dimas.brosalin.streaming.json.JsonRelationService;
import com.dimas.brosalin.streaming.json.metamodel.MetaColumn;
import com.dimas.brosalin.streaming.json.metamodel.MetaTable;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.generic.GenericData;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.*;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Created by DmitriyBrosalin on 10/05/2020.
 */
public class JsonSourceServiceTest {

    private static final ObjectMapper objectMapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    private final String pathToTableInResource = JsonSourceServiceTest.class.getClassLoader()
            .getResource("ExampleTable.json").getPath();
    private final String pathToMetaInResources = JsonSourceServiceTest.class.getClassLoader()
            .getResource("ExampleTypes.json").getPath();

    @Test
    public void jsonMetaIterationCorrectness() throws IOException {
        MetaTable metaTable = objectMapper.readValue(new File(pathToMetaInResources), MetaTable.class);
        List<StructField> structFieldList = JsonRelationService.structFieldListProvider(metaTable);
        structFieldList.forEach(System.out::println);
    }

    @Test
    public void jsonMetaIterationCorrectnessAllKeysPresent() throws IOException {
        MetaTable metaTable = objectMapper.readValue(new File(pathToMetaInResources), MetaTable.class);
        List<StructField> structFieldList = JsonRelationService.structFieldListProvider(metaTable);
        structFieldList.forEach(field -> {
            System.out.println(String.format("key: %s, type: %s", field.name(), field.dataType().typeName()));
            Optional<String> key = metaTable.getMetaColumnList()
                    .stream()
                    .map(MetaColumn::getName)
                    .filter(col -> col.equalsIgnoreCase(field.name()))
                    .findFirst();
            assertTrue(key.isPresent());
        });
    }

    @Test
    public void jsonMetaIterationCorrectnessAllTypesMatches() throws IOException {
        MetaTable metaTable = objectMapper.readValue(new File(pathToMetaInResources), MetaTable.class);
        List<StructField> structFieldList = JsonRelationService.structFieldListProvider(metaTable);
        structFieldList.forEach(field -> {
            Optional<MetaColumn> column = metaTable.getMetaColumnList()
                    .stream()
                    .filter(col -> col.getName().equalsIgnoreCase(field.name()))
                    .findFirst();
            assertTrue(column.isPresent());
            if(field.dataType() instanceof DecimalType) {
                assertTrue(column.get().getType().getType().equalsIgnoreCase("DecimalType"));
            }else if(field.dataType() instanceof TimestampType){
                assertTrue(column.get().getType().getType().equalsIgnoreCase("TimestampType"));
            }else if(field.dataType() instanceof BooleanType){
                assertTrue(column.get().getType().getType().equalsIgnoreCase("BooleanType"));
            }else if(field.dataType() instanceof FloatType){
                assertTrue(column.get().getType().getType().equalsIgnoreCase("FloatType"));
            }else if(field.dataType() instanceof LongType){
                assertTrue(column.get().getType().getType().equalsIgnoreCase("LongType"));
            }else if(field.dataType() instanceof StringType){
                assertTrue(column.get().getType().getType().equalsIgnoreCase("StringType"));
            }else{
                fail();
            }
        });
    }

    @Test
    public void jsonTableIterationCorrectness() throws IOException {
        JsonNode jsonNode = objectMapper.readTree(new File(pathToTableInResource));
        MetaTable metaTable = objectMapper.readValue(new File(pathToMetaInResources), MetaTable.class);
        List<StructField> structFieldList = JsonRelationService.structFieldListProvider(metaTable);
        Iterator<JsonNode> jsonIter =  jsonNode.get("values").elements();
        List<Row> rowList = new ArrayList<>();
        while (jsonIter.hasNext()){
            JsonNode curNode = jsonIter.next();
            System.out.println(curNode);
            Object[] objects = new Object[structFieldList.size()];
            structFieldList.forEach(field -> {
                int indx = structFieldList.indexOf(field);
                if(field.dataType() instanceof DecimalType) {
                    objects[indx] = new BigDecimal(curNode.get(field.name()).asText());
                }else if(field.dataType() instanceof TimestampType){
                    objects[indx] = Timestamp.valueOf(curNode.get(field.name()).asText());
                }else if(field.dataType() instanceof BooleanType){
                    objects[indx] = Boolean.valueOf(curNode.get(field.name()).asText());
                }else if(field.dataType() instanceof FloatType){
                    objects[indx] = Float.valueOf(curNode.get(field.name()).asText());
                }else if(field.dataType() instanceof LongType){
                    objects[indx] = Long.valueOf(curNode.get(field.name()).asText());
                }else if(field.dataType() instanceof StringType){
                    objects[indx] = curNode.get(field.name()).asText();
                }
            });
            rowList.add(RowFactory.create(objects));
        }
        System.out.println(rowList);
    }

}
