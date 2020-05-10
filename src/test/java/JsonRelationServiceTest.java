import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;

/**
 * Created by DmitriyBrosalin on 10/05/2020.
 */
public class JsonRelationServiceTest {

    private static final ObjectMapper objectMapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    private final String pathToTableInResource = JsonRelationServiceTest.class.getClassLoader()
            .getResource("ExampleTable.json").getPath();


    public void jsonIterationCorrectness() throws IOException {

        JsonNode jsonNode = objectMapper.readTree(new File(pathToTableInResource));

    }

}
