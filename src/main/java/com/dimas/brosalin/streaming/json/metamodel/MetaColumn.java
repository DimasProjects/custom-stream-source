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
public class MetaColumn {

    @JsonProperty("name")
    private String name;

    @JsonProperty("type")
    private MetaType type;

}
