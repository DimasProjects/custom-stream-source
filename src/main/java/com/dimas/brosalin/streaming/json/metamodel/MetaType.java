package com.dimas.brosalin.streaming.json.metamodel;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Created by DmitriyBrosalin on 10/05/2020.
 */
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class MetaType {

    @JsonProperty("type")
    private String type;

    @JsonProperty("scale")
    private int scale;

    @JsonProperty("precision")
    private int precision;

}
