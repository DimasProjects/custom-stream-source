package com.dimas.brosalin.streaming.json.metamodel;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Created by DmitriyBrosalin on 10/05/2020.
 */


public class MetaColumn {

    @JsonProperty("name")
    private String name;

    @JsonProperty("type")
    private MetaType type;

    public MetaColumn(String name, MetaType type) {
        this.name = name;
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public MetaType getType() {
        return type;
    }
}
