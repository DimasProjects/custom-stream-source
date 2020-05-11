package com.dimas.brosalin.streaming.json;

import com.dimas.brosalin.streaming.json.metamodel.MetaTable;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.execution.streaming.LongOffset;
import org.apache.spark.sql.execution.streaming.Offset;
import org.apache.spark.sql.execution.streaming.Source;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.Tuple2;
import scala.runtime.AbstractFunction0;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by DmitriyBrosalin on 10/05/2020.
 */
public class JsonSource implements Source {

    private final SQLContext sqlContext;
    private final Map<String, String> parameters;
    private List<Tuple2<Row, LongOffset>> internalCache = new ArrayList<>();
    private static Logger log = LoggerFactory.getLogger(JsonSource.class);
    private LongOffset offset = new LongOffset(-1);
    private boolean stopFlag = true;
    private Thread dataThread = dataGeneratorStartingThread();

    public JsonSource(SQLContext sqlContext, Map<String, String> parameters){
        this.sqlContext = sqlContext;
        this.parameters = parameters;
    }

    @Override
    public StructType schema() {
        return new StructType(createStructFieldList());
    }

    @Override
    public synchronized Option<Offset> getOffset() {
        log.info("Current offset: {}", offset.offset());
        if(offset.offset() == -1) return null;
        else return Option.apply(offset);
    }

    @Override
    public Dataset<Row> getBatch(Option<Offset> start, Offset end) {
        log.info("Came start offset: {}, Came end offset: {}", start.get(), end);
        LongOffset lower = LongOffset.convert(start.get()).getOrElse(new AbstractFunction0<LongOffset>() {
            @Override
            public LongOffset apply() {
                return new LongOffset(-1);
            }
        }).$plus(1);
        LongOffset upper = LongOffset.convert(end).getOrElse(new AbstractFunction0<LongOffset>() {
            @Override
            public LongOffset apply() {
                return new LongOffset(-1);
            }
        }).$plus(1);
        log.info("Got lower bound of batch: {}, Got upper bound of batch: {}", lower.offset(), upper.offset());
        List<Row> rows = internalCache.stream()
                .filter(tuple -> tuple._2.offset() >= lower.offset() && tuple._2.offset() <= upper.offset())
                .map(tuple -> tuple._1)
                .collect(Collectors.toList());
        return sqlContext.createDataFrame(rows, schema());
    }

    @Override
    public void commit(Offset end) {
        LongOffset lower = LongOffset.convert(end).getOrElse(new AbstractFunction0<LongOffset>() {
            @Override
            public LongOffset apply() {
                return new LongOffset(-1);
            }
        }).$plus(1);
        log.info("Got last commit: {}", lower.offset());
        List<Tuple2<Row, LongOffset>> memoryCompact = internalCache.stream()
                .filter(tuple -> tuple._2.offset() >= lower.offset())
                .collect(Collectors.toList());
        log.info("Compacted internal memory cache from size: {}, to size: {}", internalCache.size(), memoryCompact.size());
        internalCache = memoryCompact;
    }

    @Override
    public void stop() {
        stopFlag = true;
        try {
            dataThread.join(100);
        } catch (InterruptedException e) {
            log.info("Exception has been occured while shutting down internal data generator thread: {}", e.getMessage());
        }
    }

    private Thread dataGeneratorStartingThread(){
        Optional<JsonNode> jsonNode = JsonRelationService.tableContentProvider(Optional.ofNullable(parameters.get("path")));
        Iterator<JsonNode> tableContent = jsonNode.get().get("value").elements();
        Thread thread = new Thread() {
            @SneakyThrows
            @Override
            public void run() {
                while(tableContent.hasNext() || stopFlag) {
                    try {
                        synchronized (this) {
                            JsonNode curNode = tableContent.next();
                            Row row = JsonRelationService.rowProvider(curNode, Arrays.asList(createStructFieldList()));
                            offset = offset.$plus(1);
                            internalCache.add(new Tuple2<>(row, offset));
                        }
                    }catch (Exception ex){
                        log.info("Exception has been occured in internal thread dataGenerator: {}", ex.getMessage());
                    }
                    Thread.sleep(1000);
                }
            }
        };
        thread.start();
        return thread;
    }

    private StructField[] createStructFieldList(){
        Optional<MetaTable> metaTable = JsonRelationService.metaTableProvider(Optional.ofNullable(parameters.get("schema")));
        List<StructField> fieldList = metaTable.map(JsonRelationService::structFieldListProvider)
                .orElseGet(() -> JsonRelationService.structFieldListProvider(Optional.ofNullable(parameters.get("path"))));
        StructField[] structFieldArray = new StructField[fieldList.size()];
        structFieldArray = fieldList.toArray(structFieldArray);
        return structFieldArray;
    }
}
