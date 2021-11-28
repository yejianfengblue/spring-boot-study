package com.yejianfengblue.spring.boot.jackson;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.exc.InvalidDefinitionException;

import org.junit.jupiter.api.Test;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

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

    @Getter
    @ToString
    static class User_usernameSetterWithJsonIgnore {

        private String username;

        private int money;

        @JsonCreator
        public User_usernameSetterWithJsonIgnore(@JsonProperty("username") String username,
                                                 @JsonProperty("money") int money) {
            this.username = username;
            this.money = money;
        }

        @JsonGetter
        public String getUsername() {
            return username;
        }

        @JsonIgnore
        private void setUsername(String username) {
            this.username = username;
        }

        @JsonGetter
        public int getMoney() {
            return money;
        }

        @JsonSetter
        public void setMoney(int money) {
            this.money = money;
        }
    }

    @Test
    void givenObjectMapperDisableInferPropertyMutatorsDisableFailOnUnknownProperties_whenUpdateValue_thenNoFallbackSetter() throws JsonProcessingException {

        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.disable(MapperFeature.INFER_PROPERTY_MUTATORS);
        objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

        User_usernameSetterWithJsonIgnore createdUser = objectMapper.readValue(
                "{ \"username\": \"a\", \"money\": 1 }", User_usernameSetterWithJsonIgnore.class);

        User_usernameSetterWithJsonIgnore oldUser = new User_usernameSetterWithJsonIgnore("a", 1);
        User_usernameSetterWithJsonIgnore userPatch = new User_usernameSetterWithJsonIgnore("b", 2);
        log.info("oldUser json = {}", objectMapper.writeValueAsString(oldUser));

        assertThatThrownBy(
            () -> objectMapper.updateValue(oldUser, userPatch))
            .isInstanceOf(InvalidDefinitionException.class)
            .hasMessageContaining("No fallback setter/field defined for creator property 'username'");
    }
}
