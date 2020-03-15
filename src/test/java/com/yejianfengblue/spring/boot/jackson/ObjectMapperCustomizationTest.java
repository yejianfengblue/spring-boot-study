package com.yejianfengblue.spring.boot.jackson;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

@Slf4j
class ObjectMapperCustomizationTest {

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    private static class Car {

        private String color;

        private String type;
    }

    @Test
    void ignoreUnknowPropertiesBySetting_FAIL_ON_UNKNOWN_PROPERTIES_to_false() {

        ObjectMapper objectMapper = new ObjectMapper();

        assertThatThrownBy(() -> objectMapper.readValue(
                "{\n" +
                        "  \"color\" : \"yellow\",\n" +
                        "  \"type\" : \"Bumblebee\",\n" +
                        "  \"year\" : \"2000\"\n" +
                        "}",
                Car.class))
                .isInstanceOf(UnrecognizedPropertyException.class);

        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        assertDoesNotThrow(() -> objectMapper.readValue(
                "{\n" +
                        "  \"color\" : \"yellow\",\n" +
                        "  \"type\" : \"Bumblebee\",\n" +
                        "  \"year\" : \"2000\"\n" +
                        "}",
                Car.class));
    }

    static class CustomCarSerializer extends StdSerializer<Car> {

        private static final long serialVersionUID = -8489446380827988243L;

        public CustomCarSerializer(Class<Car> t) {
            super(t);
        }

        @Override
        public void serialize(Car value, JsonGenerator gen, SerializerProvider provider) throws IOException {

            gen.writeStartObject();  // {
            gen.writeStringField("brand", value.getType());
            gen.writeEndObject();  // }
        }
    }

    @Test
    void customSerializer() throws JsonProcessingException {

        ObjectMapper objectMapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addSerializer(Car.class, new CustomCarSerializer(Car.class));
        objectMapper.registerModule(module);

        assertThat(objectMapper.writeValueAsString(new Car("yellow", "Bumblebee")))
                .isEqualTo("{\"brand\":\"Bumblebee\"}");
    }

    static class CustomDeserializer extends StdDeserializer<Car> {

        private static final long serialVersionUID = 8144738014522267574L;

        public CustomDeserializer(Class<?> vc) {
            super(vc);
        }

        @Override
        public Car deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException, JsonProcessingException {

            Car car = new Car();

            ObjectCodec objectCodec = jsonParser.getCodec();
            JsonNode jsonNode = objectCodec.readTree(jsonParser);

            car.setColor(jsonNode.get("color").asText().toUpperCase());
            car.setType(jsonNode.get("type").asText().toUpperCase());

            return car;
        }
    }

    @Test
    void customDeserializer() throws JsonProcessingException {

        ObjectMapper objectMapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addDeserializer(Car.class, new CustomDeserializer(Car.class));
        objectMapper.registerModule(module);

        assertThat(objectMapper.readValue("{\n" +
                "  \"color\" : \"yellow\",\n" +
                "  \"type\" : \"Bumblebee\"\n" +
                "}", Car.class))
                .isEqualTo(new Car("YELLOW", "BUMBLEBEE"));
    }

    @Test
    void handleJava8Time() throws JsonProcessingException {

        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        assertThat(objectMapper.writeValueAsString(LocalDateTime.of(2000, 1, 1, 1, 2, 3)))
                .isEqualTo("\"2000-01-01T01:02:03\"");
    }

    @Test
    void test () throws JsonProcessingException {

        String jsonCarArray =
                "[{ \"color\" : \"Black\", \"type\" : \"BMW\" }, { \"color\" : \"Red\", \"type\" : \"FIAT\" }]";
        ObjectMapper objectMapper = new ObjectMapper();
//        objectMapper.configure(DeserializationFeature.USE_JAVA_ARRAY_FOR_JSON_ARRAY, true);
        Car[] cars = objectMapper.readValue(jsonCarArray, Car[].class);
    }

}
