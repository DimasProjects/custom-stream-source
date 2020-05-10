package com.dimas.brosalin.streaming.json.metamodel;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Created by DmitriyBrosalin on 10/05/2020.
 */
public class MetaType {

    @JsonProperty("name")
    private String name;

    @JsonProperty("scale")
    private int scale;

    @JsonProperty("precision")
    private int precision;

    public MetaType(String name, short scale, short precision) {

        this.name = name;
        this.scale = scale;
        this.precision = precision;

    }

    public String getName() {
        return name;
    }

    public int getScale() {
        return scale;
    }

    public int getPrecision() {
        return precision;
    }
}
