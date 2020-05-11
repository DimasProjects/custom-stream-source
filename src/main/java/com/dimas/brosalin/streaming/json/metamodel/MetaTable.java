package com.dimas.brosalin.streaming.json.metamodel;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;

/**
 * Created by DmitriyBrosalin on 10/05/2020.
 */
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class MetaTable {

    @JsonProperty("name")
    private String name;

    @JsonProperty("columns")
    private List<MetaColumn> metaColumnList;

}
