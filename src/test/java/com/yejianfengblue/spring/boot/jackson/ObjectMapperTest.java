package com.yejianfengblue.spring.boot.jackson;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.SerializationFeature;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
class ObjectMapperTest {

    ObjectMapper objectMapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    private static class Car {

        private String color;

        private String type;
    }

    @Test
    void writeValueAsString() throws JsonProcessingException {

        assertThat(objectMapper.writeValueAsString(new Car("yellow", "Bumblebee")))
                .isEqualTo("{\n" +
                        "  \"color\" : \"yellow\",\n" +
                        "  \"type\" : \"Bumblebee\"\n" +
                        "}");
    }

    @Test
    void readValue() throws JsonProcessingException {

        assertThat(objectMapper.readValue("{\n" +
                "  \"color\" : \"yellow\",\n" +
                "  \"type\" : \"Bumblebee\"\n" +
                "}", Car.class))
                .isEqualTo(new Car("yellow", "Bumblebee"));
    }

    @Test
    void jsonToJsonNode() throws JsonProcessingException {

        JsonNode jsonNode = objectMapper.readTree("{\n" +
                "  \"color\" : \"yellow\",\n" +
                "  \"type\" : \"Bumblebee\"\n" +
                "}");
        assertThat(jsonNode.get("color").asText()).isEqualTo("yellow");
        assertThat(jsonNode.get("type").asText()).isEqualTo("Bumblebee");
    }

    @Test
    void jsonArrayToJavaList() throws JsonProcessingException {

        assertThat(objectMapper.readValue("[\n" +
                "{\n" +
                "  \"color\" : \"yellow\",\n" +
                "  \"type\" : \"Bumblebee\"\n" +
                "}," +
                "{\n" +
                "  \"color\" : \"red\",\n" +
                "  \"type\" : \"Optimus\"\n" +
                "}" +
                "]\n", new TypeReference<List<Car>>() {}
        )).isEqualTo(
                List.of(new Car("yellow", "Bumblebee"),
                        new Car("red", "Optimus")));
    }

    @Test
    void jsonToJavaMap() throws JsonProcessingException {

        assertThat(objectMapper.readValue(
                "{\n" +
                        "  \"color\" : \"yellow\",\n" +
                        "  \"type\" : \"Bumblebee\"\n" +
                        "}",
                new TypeReference<Map<String, Object>>() {}
        )).isEqualTo(
                Map.of("color", "yellow", "type", "Bumblebee"));
    }


    @Getter
    @ToString
    @AllArgsConstructor
    static class PojoWithReadonlyId {

        private Integer id;

        private String desc;

        // do nothing to make id not updatable
        private void setId(Integer id) {}

        public void setDesc(String desc) {
            this.desc = desc;
        }
    }

    @Test
    void objectMapperUpdateValueRespectsSetter() throws IOException {

        PojoWithReadonlyId oldObject = new PojoWithReadonlyId(1, "a");
        PojoWithReadonlyId patch = new PojoWithReadonlyId(2, "b");

        PojoWithReadonlyId newObject = objectMapper.updateValue(oldObject, patch);
        // in-place update for mutable object
        assertThat(newObject).isSameAs(oldObject);
        assertThat(newObject.getId()).isEqualTo(1);
        assertThat(newObject.getDesc()).isEqualTo("b");

        PojoWithReadonlyId nullPatch = new PojoWithReadonlyId(3, null);
        PojoWithReadonlyId nullUpdatedObject = objectMapper.updateValue(oldObject, nullPatch);
        assertThat(nullUpdatedObject).isSameAs(oldObject);
        assertThat(nullUpdatedObject.getId()).isEqualTo(1);
        assertThat(nullUpdatedObject.getDesc()).isNull();
    }
}
