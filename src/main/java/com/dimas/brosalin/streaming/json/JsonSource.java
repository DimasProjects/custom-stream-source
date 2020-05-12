package com.dimas.brosalin.streaming.json;

import com.dimas.brosalin.streaming.json.metamodel.MetaTable;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.SneakyThrows;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.streaming.LongOffset;
import org.apache.spark.sql.execution.streaming.Offset;
import org.apache.spark.sql.execution.streaming.Source;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;
import scala.runtime.AbstractFunction0;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by DmitriyBrosalin on 10/05/2020.
 */
public class JsonSource implements Source {

    private SQLContext sqlContext;
    private Map<String, String> parameters;
    private List<Tuple2<InternalRow, LongOffset>> internalCache = new ArrayList<>();
    private static Logger log = LoggerFactory.getLogger(JsonSource.class);
    private LongOffset offset = new LongOffset(-1);
    private boolean stopFlag = true;
    private Thread dataThread;

    public JsonSource(SQLContext sqlContext, Map<String, String> parameters){
        System.out.println("INSIDE CONSTRUCTOR");
        this.sqlContext = sqlContext;
        this.parameters = parameters;
        this.dataThread = dataGeneratorStartingThread();
        System.out.println("INSIDE CONSTRUCTOR, MAP: " + parameters);
    }

    @Override
    public StructType schema() {
        return new StructType(createStructFieldList());
    }

    @Override
    public synchronized Option<Offset> getOffset() {
//        log.info("Current offset: {}", offset.offset());
        if(offset.offset() == -1) return scala.Option.apply(null);
        else return Option.apply(offset);
    }

    @Override
    public Dataset<Row> getBatch(Option<Offset> start, Offset end) {
        LongOffset lower = LongOffset.convert(start.getOrElse(new AbstractFunction0<LongOffset>() {
            @Override
            public LongOffset apply() {
                return new LongOffset(-1);
            }
        })).getOrElse(new AbstractFunction0<LongOffset>() {
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
        List<InternalRow> rows = internalCache.stream()
                .filter(tuple -> tuple._2.offset() >= lower.offset() && tuple._2.offset() <= upper.offset())
                .map(tuple -> tuple._1)
                .collect(Collectors.toList());
        ClassTag<InternalRow> tag = scala.reflect.ClassTag$.MODULE$.apply(InternalRow.class);
        RDD<InternalRow> internalRowRDD = sqlContext.sparkContext().parallelize(
                JavaConversions.asScalaBuffer(rows),
                sqlContext.sparkContext().defaultParallelism(),
                tag);
        return sqlContext.internalCreateDataFrame(internalRowRDD, schema(), true);
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
        List<Tuple2<InternalRow, LongOffset>> memoryCompact = internalCache.stream()
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
        System.out.println("INSIDE dataGeneratorStartingThread");
        System.out.println("PATH TO JSON TABLE: " + parameters);
        Optional<JsonNode> jsonNode = JsonRelationService.tableContentProvider(Optional.ofNullable(parameters.get("path")));
        System.out.println("GOT OPTIONAL jsonNode: " + jsonNode.get());
//        Iterator<JsonNode> tableContent = jsonNode.get().get("values").elements();
        Thread thread = new Thread() {
            @Override
            public void run() {
                long part = 0;
                for (int i = 0; i < 5; i++) {
                    Iterator<JsonNode> tableContent = jsonNode.get().get("values").elements();
                    while (tableContent.hasNext()) {
                        try {
//                        while(tableContent.hasNext()) {
                            synchronized (this) {
                                JsonNode curNode = tableContent.next();
                                InternalRow row = JsonRelationService.internalRowProvider(curNode, Arrays.asList(createStructFieldList()));
//                            Row row = JsonRelationService.rowProvider(curNode, Arrays.asList(createStructFieldList()));
                                log.info("Current row: " + row);
                                part += 1;
//                                offset = offset.$plus(1);
                                internalCache.add(new Tuple2<>(row, LongOffset.apply(part)));
                                Thread.sleep(100);
                            }
//                        }
                        } catch (Exception ex) {
                            log.info("Exception has been occured in internal thread dataGenerator: {}", ex.getMessage());
                        }
                    }
//                offset = offset.$plus(1);
                    offset = LongOffset.apply(part);
                    log.info("Offset updated: " + offset.offset());
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
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
        log.info("Struct fields: " + fieldList);
        return structFieldArray;
    }

    public static StructType createStructType(Map<String, String> parameters){
        Optional<MetaTable> metaTable = JsonRelationService.metaTableProvider(Optional.ofNullable(parameters.get("schema")));
        List<StructField> fieldList = metaTable.map(JsonRelationService::structFieldListProvider)
                .orElseGet(() -> JsonRelationService.structFieldListProvider(Optional.ofNullable(parameters.get("path"))));
        StructField[] structFieldArray = new StructField[fieldList.size()];
        structFieldArray = fieldList.toArray(structFieldArray);
        log.info("Struct fields: " + fieldList);
        return new StructType(structFieldArray);
    }
}
