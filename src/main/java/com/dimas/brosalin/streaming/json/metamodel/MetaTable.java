package com.dimas.brosalin.streaming.json.metamodel;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Created by DmitriyBrosalin on 10/05/2020.
 */
public class MetaTable {

    @JsonProperty("name")
    private String name;

    @JsonProperty("columns")
    private List<MetaColumn> metaColumnList;

    public MetaTable(String name, List<MetaColumn> metaColumns){

        this.name = name;
        this.metaColumnList = metaColumns;

    }

    public List<MetaColumn> getMetaColumnList() {
        return metaColumnList;
    }

    public String getName() {
        return name;
    }
}
