package com.yejianfengblue.spring.boot.jackson;

import com.fasterxml.jackson.annotation.*;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.InvalidDefinitionException;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import static com.yejianfengblue.spring.boot.jackson.JsonSchemaUtil.assertCouldDeserialize;
import static com.yejianfengblue.spring.boot.jackson.JsonSchemaUtil.assertCouldSerialize;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

/**
 * Test how Jackson decides whether a field is serializable and deserializable
 */
@Slf4j
public class SerDeserializableDecisionTest {


    // by default, if no getters or setters, only public field is serialized
    @ToString
    private static class PojoWithoutGetterSetter {

        private String privateField;

        protected String protectedField;

        String packageField;

        public String publicField;

        // NO getters or setters
    }

    @SneakyThrows
    @Test
    void givenPojoWithoutGetterOrSetter_thenOnlyPublicFieldIsSerializable() {

        ObjectMapper objectMapper = new ObjectMapper();

        JsonNode jsonNode = objectMapper.valueToTree(new PojoWithoutGetterSetter());
        assertThat(jsonNode.has("privateField")).isFalse();
        assertThat(jsonNode.has("protectedField")).isFalse();
        assertThat(jsonNode.has("packageField")).isFalse();
        assertThat(jsonNode.has("publicField")).isTrue();

        assertCouldSerialize(objectMapper, PojoWithoutGetterSetter.class.getDeclaredField("privateField")).isFalse();
        assertCouldSerialize(objectMapper, PojoWithoutGetterSetter.class.getDeclaredField("protectedField")).isFalse();
        assertCouldSerialize(objectMapper, PojoWithoutGetterSetter.class.getDeclaredField("packageField")).isFalse();
        assertCouldSerialize(objectMapper, PojoWithoutGetterSetter.class.getDeclaredField("publicField")).isTrue();
    }

    @SneakyThrows
    @Test
    void givenPojoWithoutGetterOrSetter_thenOnlyPublicFieldIsDeserializable() {

        ObjectMapper objectMapper = new ObjectMapper();

        assertThatThrownBy(() -> objectMapper.readValue(
                "{" +
                        "\"privateField\" : \"1\", " +
                        "\"protectedField\" : \"2\", " +
                        "\"packageField\" : \"3\", " +
                        "\"publicField\" : \"4\"" +
                        "}", PojoWithoutGetterSetter.class))
                .isInstanceOf(UnrecognizedPropertyException.class);

        PojoWithoutGetterSetter pojo = objectMapper.readValue(
                "{\n" +
                        "\"publicField\" : \"4\"" +
                        "}", PojoWithoutGetterSetter.class);
        assertThat(pojo.publicField).isEqualTo("4");

        assertCouldDeserialize(objectMapper, PojoWithoutGetterSetter.class.getDeclaredField("privateField")).isFalse();
        assertCouldDeserialize(objectMapper, PojoWithoutGetterSetter.class.getDeclaredField("protectedField")).isFalse();
        assertCouldDeserialize(objectMapper, PojoWithoutGetterSetter.class.getDeclaredField("packageField")).isFalse();
        assertCouldDeserialize(objectMapper, PojoWithoutGetterSetter.class.getDeclaredField("publicField")).isTrue();
    }

    // A getter makes a non-public field serializable and deserializable
    private static class PojoPrivateFieldHavingGetter {

        @Getter
        private String privateFieldWithGetter;

        private String privateFieldWithoutGetter;
    }

    @SneakyThrows
    @Test
    void givenPrivateFieldHavingGetter_thenSerializable() {

        ObjectMapper objectMapper = new ObjectMapper();

        JsonNode jsonNode = objectMapper.valueToTree(new PojoPrivateFieldHavingGetter());
        assertThat(jsonNode.has("privateFieldWithGetter")).isTrue();
        assertThat(jsonNode.has("privateFieldWithoutGetter")).isFalse();

        assertCouldSerialize(objectMapper, PojoPrivateFieldHavingGetter.class.getDeclaredField("privateFieldWithGetter")).isTrue();
        assertCouldSerialize(objectMapper, PojoPrivateFieldHavingGetter.class.getDeclaredField("privateFieldWithoutGetter")).isFalse();
    }

    @SneakyThrows
    @Test
    void givenPrivateFieldHavingGetter_thenDeserializable() {

        ObjectMapper objectMapper = new ObjectMapper();

        assertThatThrownBy(() -> objectMapper.readValue(
                "{" +
                        "\"privateFieldWithGetter\" : \"1\", " +
                        "\"privateFieldWithoutGetter\" : \"2\"" +
                        "}", PojoPrivateFieldHavingGetter.class
        )).isInstanceOf(UnrecognizedPropertyException.class);

        PojoPrivateFieldHavingGetter pojo = objectMapper.readValue(
                "{" +
                        "\"privateFieldWithGetter\" : \"1\"" +
                        "}", PojoPrivateFieldHavingGetter.class);
        assertThat(pojo.getPrivateFieldWithGetter()).isEqualTo("1");

        assertCouldDeserialize(objectMapper, PojoPrivateFieldHavingGetter.class.getDeclaredField("privateFieldWithGetter")).isTrue();
        assertCouldDeserialize(objectMapper, PojoPrivateFieldHavingGetter.class.getDeclaredField("privateFieldWithoutGetter")).isFalse();
    }

    // A setter makes a non-public field deserializable only
    private static class PojoPrivateFieldHavingSetter {

        @Setter
        private String privateFieldWithSetter;

        private String privateFieldWithoutSetter;

        private String accessFieldWithSetter() {
            return privateFieldWithSetter;
        }
    }

    @SneakyThrows
    @Test
    void givenPrivateFieldHavingSetter_thenNotSerializable() {

        ObjectMapper objectMapper = new ObjectMapper();

        assertThat(objectMapper.canSerialize(PojoPrivateFieldHavingSetter.class)).isFalse();
        assertThatThrownBy(() -> objectMapper.writeValueAsString(new PojoPrivateFieldHavingSetter()))
                .isInstanceOf(InvalidDefinitionException.class)
                .hasMessageContaining("No serializer found");

        assertCouldSerialize(objectMapper, PojoPrivateFieldHavingSetter.class.getDeclaredField("privateFieldWithSetter")).isFalse();
        assertCouldSerialize(objectMapper, PojoPrivateFieldHavingSetter.class.getDeclaredField("privateFieldWithoutSetter")).isFalse();
    }

    @SneakyThrows
    @Test
    void givenPrivateFieldHavingSetter_thenDeserializable() {

        ObjectMapper objectMapper = new ObjectMapper();

        assertThatThrownBy(() -> objectMapper.readValue(
                "{" +
                        "\"privateFieldWithSetter\" : \"1\", " +
                        "\"privateFieldWithoutSetter\" : \"2\"" +
                        "}", PojoPrivateFieldHavingSetter.class
        )).isInstanceOf(UnrecognizedPropertyException.class);

        PojoPrivateFieldHavingSetter pojo = objectMapper.readValue(
                "{" +
                        "\"privateFieldWithSetter\" : \"1\"" +
                        "}", PojoPrivateFieldHavingSetter.class);
        assertThat(pojo.accessFieldWithSetter()).isEqualTo("1");

        assertCouldDeserialize(objectMapper, PojoPrivateFieldHavingSetter.class.getDeclaredField("privateFieldWithSetter")).isTrue();
        assertCouldDeserialize(objectMapper, PojoPrivateFieldHavingSetter.class.getDeclaredField("privateFieldWithoutSetter")).isFalse();
    }

    // ObjectMapper auto detect visibility is configurable

    @SneakyThrows
    @Test
    void givenNonPublicFieldWithoutGetterSetter_whenEnableObjectMapperAutoDetect_thenSerializable() {

        ObjectMapper objectMapper = new ObjectMapper();
        // when
        objectMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.NON_PRIVATE);

        // then
        JsonNode jsonNode = objectMapper.valueToTree(new PojoWithoutGetterSetter());
        assertThat(jsonNode.has("privateField")).isFalse();
        assertThat(jsonNode.has("protectedField")).isTrue();
        assertThat(jsonNode.has("packageField")).isTrue();
        assertThat(jsonNode.has("publicField")).isTrue();

        assertCouldSerialize(objectMapper, PojoWithoutGetterSetter.class.getDeclaredField("privateField")).isFalse();
        assertCouldSerialize(objectMapper, PojoWithoutGetterSetter.class.getDeclaredField("protectedField")).isTrue();
        assertCouldSerialize(objectMapper, PojoWithoutGetterSetter.class.getDeclaredField("packageField")).isTrue();
        assertCouldSerialize(objectMapper, PojoWithoutGetterSetter.class.getDeclaredField("publicField")).isTrue();
    }

    @SneakyThrows
    @Test
    void givenNonPublicFieldWithoutGetterSetter_whenEnableObjectMapperAutoDetect_thenDeserializable() {

        ObjectMapper objectMapper = new ObjectMapper();

        // when
        objectMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.NON_PRIVATE);

        // then
        assertDoesNotThrow(() -> objectMapper.readValue(
                "{" +
                        "\"protectedField\" : \"2\", " +
                        "\"packageField\" : \"3\", " +
                        "\"publicField\" : \"4\"" +
                        "}", PojoWithoutGetterSetter.class));

        assertCouldDeserialize(objectMapper, PojoWithoutGetterSetter.class.getDeclaredField("privateField")).isFalse();
        assertCouldDeserialize(objectMapper, PojoWithoutGetterSetter.class.getDeclaredField("protectedField")).isTrue();
        assertCouldDeserialize(objectMapper, PojoWithoutGetterSetter.class.getDeclaredField("packageField")).isTrue();
        assertCouldDeserialize(objectMapper, PojoWithoutGetterSetter.class.getDeclaredField("publicField")).isTrue();
    }

    private static class PojoPrivateFieldNotSerializableButDeserializable {

        @Getter
        @Setter
        private String username;  // without fields other than "password", Jackson complains no serializer found

        private String password;

        @JsonIgnore
        String getPassword() {
            return password;
        }

        @JsonProperty
        void setPassword(String password) {
            this.password = password;
        }
    }

    @SneakyThrows
    @Test
    void givenPrivateFieldGetterAnnotatedJsonIgnore_thenNotSerializable() {

        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonNode = objectMapper.valueToTree(new PojoPrivateFieldNotSerializableButDeserializable());

        assertThat(jsonNode.has("username")).isTrue();
        assertThat(jsonNode.has("password")).isFalse();

        assertCouldSerialize(objectMapper, PojoPrivateFieldNotSerializableButDeserializable.class.getDeclaredField("username")).isTrue();
        assertCouldSerialize(objectMapper, PojoPrivateFieldNotSerializableButDeserializable.class.getDeclaredField("password")).isFalse();
    }

    @SneakyThrows
    @Test
    void givenPrivateFieldSetterAnnotatedJsonProperty_thenDeserializable() {

        ObjectMapper objectMapper = new ObjectMapper();

        PojoPrivateFieldNotSerializableButDeserializable pojo = objectMapper.readValue(
                "{" +
                        "\"username\" : \"chris\", " +
                        "\"password\" : \"christina\"" +
                        "}", PojoPrivateFieldNotSerializableButDeserializable.class
        );

        assertThat(pojo.getUsername()).isEqualTo("chris");
        assertThat(pojo.getPassword()).isEqualTo("christina");

        assertCouldDeserialize(objectMapper, PojoPrivateFieldNotSerializableButDeserializable.class.getDeclaredField("username")).isTrue();
        assertCouldDeserialize(objectMapper, PojoPrivateFieldNotSerializableButDeserializable.class.getDeclaredField("password")).isTrue();
    }

    @SneakyThrows
    @Test
    void givenPrivateFieldHavingGetter_whenDisableInferPropertyMutators_thenNotDeserializable() {

        ObjectMapper objectMapper = new ObjectMapper();

        // when
        objectMapper.disable(MapperFeature.INFER_PROPERTY_MUTATORS);

        // then
        assertThatThrownBy(() -> objectMapper.readValue(
                "{" +
                        "\"privateFieldWithGetter\" : \"1\"" +
                        "}", PojoPrivateFieldHavingGetter.class))
                .isInstanceOf(UnrecognizedPropertyException.class);

        assertCouldDeserialize(objectMapper, PojoPrivateFieldHavingGetter.class.getDeclaredField("privateFieldWithGetter")).isFalse();
    }

    @AllArgsConstructor
    @NoArgsConstructor
    private static class PojoPrivateFieldWithoutSetterAndInJsonCreator {

        private String privateField;

        private String privateFieldNotInJsonCreator;

        @JsonCreator
        PojoPrivateFieldWithoutSetterAndInJsonCreator(String privateField) {
            this.privateField = privateField;
        }
    }

    @SneakyThrows
    @Test
    void givenPrivateFieldInJsonCreator_thenNotMakeItDeserializable() {

        ObjectMapper objectMapper = new ObjectMapper();

        assertCouldDeserialize(objectMapper, PojoPrivateFieldWithoutSetterAndInJsonCreator.class.getDeclaredField("privateField")).isFalse();
    }

    @SneakyThrows
    @Test
    void givenPrivateFieldInJsonCreator_whenDisableInferPropertyMutators_thenNotMakeItDeserializable() {

        ObjectMapper objectMapper = new ObjectMapper();

        objectMapper.disable(MapperFeature.INFER_PROPERTY_MUTATORS);

        assertCouldDeserialize(objectMapper, PojoPrivateFieldWithoutSetterAndInJsonCreator.class.getDeclaredField("privateField")).isFalse();
    }

    private static class PojoPrivateFieldWithGetterWithJsonIgnoreSetter {

        private String readOnlyField;

        public PojoPrivateFieldWithGetterWithJsonIgnoreSetter() {
            readOnlyField = "initial value";
        }

        @JsonProperty
        public String getReadOnlyField() {
            return readOnlyField;
        }

        @JsonIgnore
        public void setReadOnlyField(String readOnlyField) {
            this.readOnlyField = readOnlyField;
        }
    }

    @SneakyThrows
    @Test
    void givenPrivateFieldWithGetterWithJsonIgnoreSetter_thenSerializableAndDeserializable() {

        ObjectMapper objectMapper = new ObjectMapper();

        assertCouldSerialize(objectMapper, PojoPrivateFieldWithGetterWithJsonIgnoreSetter.class.getDeclaredField("readOnlyField")).isTrue();
        assertCouldDeserialize(objectMapper, PojoPrivateFieldWithGetterWithJsonIgnoreSetter.class.getDeclaredField("readOnlyField")).isTrue();
    }

    @SneakyThrows
    @Test
    void givenPrivateFieldWithGetterWithJsonIgnoreSetter_whenDisableInferPropertyMutators_thenSerializableButNotDeserializable() {

        ObjectMapper objectMapper = new ObjectMapper();

        objectMapper.disable(MapperFeature.INFER_PROPERTY_MUTATORS);
        objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

        assertCouldSerialize(objectMapper, PojoPrivateFieldWithGetterWithJsonIgnoreSetter.class.getDeclaredField("readOnlyField")).isTrue();
        assertCouldDeserialize(objectMapper, PojoPrivateFieldWithGetterWithJsonIgnoreSetter.class.getDeclaredField("readOnlyField")).isFalse();

        PojoPrivateFieldWithGetterWithJsonIgnoreSetter pojo = new PojoPrivateFieldWithGetterWithJsonIgnoreSetter();
        assertThat(pojo.getReadOnlyField()).isEqualTo("initial value");

        PojoPrivateFieldWithGetterWithJsonIgnoreSetter updatedPojo = objectMapper.readerForUpdating(pojo).readValue(
                "{" +
                        "\"readOnlyField\" : \"updated value\"" +
                        "}");
        assertThat(pojo.getReadOnlyField()).isEqualTo("initial value");
        assertThat(updatedPojo.getReadOnlyField()).isEqualTo("initial value");
    }

    private static class PojoPrivateFieldWithGetterWithJsonIgnoreSetterAndIncludedInJsonCreator {

        private String readOnlyField;

        @JsonCreator
        public PojoPrivateFieldWithGetterWithJsonIgnoreSetterAndIncludedInJsonCreator(@JsonProperty("readOnlyField") String readOnlyField) {
            this.readOnlyField = readOnlyField;
        }

        @JsonProperty
        public String getReadOnlyField() {
            return readOnlyField;
        }

        @JsonIgnore
        public void setReadOnlyField(String readOnlyField) {
            this.readOnlyField = readOnlyField;
        }
    }

    @SneakyThrows
    @Test
    void givenPrivateFieldWithGetterWithJsonIgnoreSetterAndIncludedInJsonCreator_whenDisableInferPropertyMutators_thenSerializableButErrorOnDeserialization() {

        ObjectMapper objectMapper = new ObjectMapper();

        objectMapper.disable(MapperFeature.INFER_PROPERTY_MUTATORS);
        objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

        assertCouldSerialize(objectMapper, PojoPrivateFieldWithGetterWithJsonIgnoreSetterAndIncludedInJsonCreator.class.getDeclaredField("readOnlyField")).isTrue();
        assertCouldDeserialize(objectMapper, PojoPrivateFieldWithGetterWithJsonIgnoreSetterAndIncludedInJsonCreator.class.getDeclaredField("readOnlyField")).isTrue();

        PojoPrivateFieldWithGetterWithJsonIgnoreSetterAndIncludedInJsonCreator pojo = new PojoPrivateFieldWithGetterWithJsonIgnoreSetterAndIncludedInJsonCreator("initial");
        assertThat(pojo.getReadOnlyField()).isEqualTo("initial");

        assertThatThrownBy(() -> objectMapper.readerForUpdating(pojo).readValue(
                "{" +
                        "\"readOnlyField\" : \"updated\"" +
                        "}"))
                .isInstanceOf(InvalidDefinitionException.class)
                .hasMessageContaining("No fallback setter/field defined for creator property 'readOnlyField'");
    }

    private static class PojoPrivateFieldReadOnly {

        @JsonProperty(access = JsonProperty.Access.READ_ONLY)
        private String readOnlyField;
    }

    @SneakyThrows
    @Test
    void givenPrivateFieldAnnotatedWithJsonPropertyAccessReadOnly_thenSerializableButNotDeserializable() {

        ObjectMapper objectMapper = new ObjectMapper();

        assertCouldSerialize(objectMapper, PojoPrivateFieldReadOnly.class.getDeclaredField("readOnlyField")).isTrue();
        assertCouldDeserialize(objectMapper, PojoPrivateFieldReadOnly.class.getDeclaredField("readOnlyField")).isFalse();
    }
}
