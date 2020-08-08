package com.yejianfengblue.spring.boot.jackson;

import com.fasterxml.jackson.annotation.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.exc.InvalidDefinitionException;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;

import javax.persistence.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Slf4j
class JacksonAnnotationTest {

    ObjectMapper objectMapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

    @Data
    static class WithProperties {

        private String name;

        private Map<String, String> properties = new HashMap<>();

        @JsonAnyGetter
        public Map<String, String> getProperties() {
            return properties;
        }

        @JsonAnySetter  // all unmapped key-value pairs from JSON Object values are added using this mutator
        public void addProperty(String key, String value) {
            properties.put(key, value);
        }
    }

    @Test
    @SneakyThrows
    void serializeMapKeyValueAsStandardJsonAttribute() {

        WithProperties withProperties = new WithProperties();
        withProperties.setName("account");
        withProperties.addProperty("username", "harry");
        withProperties.addProperty("password", "potter");

        assertThat(objectMapper.writeValueAsString(withProperties))
                .isEqualTo(
                        "{\n" +
                                "  \"name\" : \"account\",\n" +
                                "  \"password\" : \"potter\",\n" +
                                "  \"username\" : \"harry\"\n" +
                                "}"
                );
    }

    @Test
    @SneakyThrows
    void deserializeUnmappedJsonAttributeToMap() {

        WithProperties withProperties = new WithProperties();
        withProperties.setName("account");
        withProperties.addProperty("username", "harry");
        withProperties.addProperty("password", "potter");

        assertThat(objectMapper.readValue(
                "{\n" +
                        "  \"name\" : \"account\",\n" +
                        "  \"password\" : \"potter\",\n" +
                        "  \"username\" : \"harry\"\n" +
                        "}"
                , WithProperties.class))
                .isEqualTo(withProperties);
    }

    @JsonPropertyOrder({"name", "id"})  // @JsonPropertyOrder has a boolean attribute "alphabetic"
    @Data
    @AllArgsConstructor
    static class ClassA {

        private int id;

        private String name;
    }

    @Test
    @SneakyThrows
    void specifyPropertiesOrderOnSerialization() {

        ClassA classA = new ClassA(1, "Ultraman");

        assertThat(objectMapper.writeValueAsString(classA))
                .isEqualTo(
                        "{\n" +
                                "  \"name\" : \"Ultraman\",\n" +
                                "  \"id\" : 1\n" +
                                "}"
                );
    }

    @Data
    @AllArgsConstructor
    static class ClassB {

        private String name;

        /* useful for injecting values already serialized in JSON or passing javascript function definitions
        from server to a javascript client. */
        @JsonRawValue
        private String raw;
    }

    @Test
    void whenSerializingUsingJsonRawValue_thenCorrect()
            throws JsonProcessingException {

        ClassB classB = new ClassB("My bean", "{\"attr\":false}");

        String result = new ObjectMapper().writeValueAsString(classB);
        log.info(result);
        // without @JsonRawValue: {"name":"My bean","raw":"{\"attr\":false}"}
        // with @JsonRawValue: {"name":"My bean","raw":{"attr":false}}
    }

///////////////////////////////////////////////////////////////////////////////

    @Getter
    @Setter
    @EqualsAndHashCode
    static class ClassC {

        private int id;

        private String username;

        @JsonCreator
        ClassC(@JsonProperty("id") int id, @JsonProperty("name") String name) {

            this.id = id;
            this.username = name;
        }
    }

    @Test
    @SneakyThrows
    void jsonCreator_specifyConstructorToInstantiateObject() {

        assertThat(objectMapper.readValue("{\"id\" : 1, \"name\" : \"Hououin Kyoma\"}", ClassC.class))
                .isEqualTo(new ClassC(1, "Hououin Kyoma"));
    }

    @Getter
    @Setter
    @EqualsAndHashCode
    @AllArgsConstructor
    static class ClassD {

        private int id;

        private String username;

        private String password;

        @JsonGetter("name")
            // no field named "name", so serialize as an additional JSON attribute
        String getName() {
            return StringUtils.capitalize(username);
        }

        @JsonGetter("password")
            // if the name matches a field, then serialize that field with this getter
        String maskPassword() {
            return password.replaceAll(".", "*");
        }
    }

    @Test
    @SneakyThrows
    void JsonGetter_isUsedToSerializeSpecifiedField() {

        assertThat(objectMapper.writeValueAsString(new ClassD(1, "mayuri", "dodoru")))
                .isEqualTo(
                        "{\n" +
                                "  \"id\" : 1,\n" +
                                "  \"username\" : \"mayuri\",\n" +
                                "  \"password\" : \"******\",\n" +
                                "  \"name\" : \"Mayuri\"\n" +
                                "}");
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    static class ClassE {

        private int id;

        private String username;

        private String password;

        @JsonSetter("name")
        void setName(String name) {
            this.username = name.toLowerCase();
        }
    }

    @Test
    @SneakyThrows
    void JsonSetter_isUsedToDeserializeSpecifiedJsonAttribute() {

        assertThat(objectMapper.readValue(
                "{\n" +
                        "  \"id\" : 1,\n" +
                        "  \"name\" : \"Mayuri\",\n" +
                        "  \"password\" : \"dodoru\"\n" +
                        "}"
                , ClassE.class))
                .isEqualTo(
                        new ClassE(1, "mayuri", "dodoru")
                );
    }

    // @JsonAlias defines one or more alternative names for a property during DEserialization
    static class ClassWithDiffNameOnJsonGetterAndJsonSetter {

        private String property;

        @JsonCreator
        public ClassWithDiffNameOnJsonGetterAndJsonSetter(@JsonProperty("property") String property) {
            this.property = property;
        }

        @JsonGetter("x-property")
        String getProperty() {
            return property;
        }

        @JsonSetter("y-property")
        void setProperty(String property) {
            this.property = property;
        }
    }

    @Test
    @SneakyThrows
    void givenJsonGetterNameDiffWithJsonSetterName_thenSerializationUseJsonGetterName_deserializationUseJsonSetterName() {

        assertThat(objectMapper.writeValueAsString(new ClassWithDiffNameOnJsonGetterAndJsonSetter("abc")))
                .isEqualTo(
                        "{\n" +
                                "  \"x-property\" : \"abc\"\n" +
                                "}"
                );

        assertThat(objectMapper.readValue(
                "{\n" +
                        "  \"property\" : \"abc\"\n" +
                        "}",
                ClassWithDiffNameOnJsonGetterAndJsonSetter.class).getProperty())
                .isEqualTo("abc");

        assertThat(objectMapper.readValue(
                "{\n" +
                        "  \"y-property\" : \"abc\"\n" +
                        "}",
                ClassWithDiffNameOnJsonGetterAndJsonSetter.class).getProperty())
                .isEqualTo("abc");
    }

    @NoArgsConstructor
    @AllArgsConstructor
    static class ClassWithPropertyAlias {

        @JsonAlias({"x-property", "y-property"})
        @Getter
        @Setter
        private String property;
    }

    @Test
    @SneakyThrows
    void JsonAlias_defineAlternativeNameDuringDeserialization() {

        assertThat(objectMapper.writeValueAsString(new ClassWithPropertyAlias("abc")))
                .isEqualTo(
                        "{\n" +
                                "  \"property\" : \"abc\"\n" +
                                "}"
                );

        assertThat(objectMapper.readValue(
                "{\n" +
                        "  \"property\" : \"abc\"\n" +
                        "}",
                ClassWithPropertyAlias.class).getProperty())
                .isEqualTo("abc");

        assertThat(objectMapper.readValue(
                "{\n" +
                        "  \"x-property\" : \"abc\"\n" +
                        "}",
                ClassWithPropertyAlias.class).getProperty())
                .isEqualTo("abc");

        assertThat(objectMapper.readValue(
                "{\n" +
                        "  \"y-property\" : \"abc\"\n" +
                        "}",
                ClassWithPropertyAlias.class).getProperty())
                .isEqualTo("abc");
    }

    @NoArgsConstructor
    @AllArgsConstructor
    static class ClassWithPropertyAliasOnJsonSetter {

        private String property;

        @JsonGetter
        String getProperty() {
            return property;
        }

        @JsonSetter
        @JsonAlias({"x-property", "y-property"})
        void setProperty(String property) {
            this.property = property;
        }
    }

    @Test
    @SneakyThrows
    void JsonAlias_onJsonSetter_defineAlternativeNameDuringDeserialization() {

        assertThat(objectMapper.writeValueAsString(new ClassWithPropertyAliasOnJsonSetter("abc")))
                .isEqualTo(
                        "{\n" +
                                "  \"property\" : \"abc\"\n" +
                                "}"
                );

        assertThat(objectMapper.readValue(
                "{\n" +
                        "  \"property\" : \"abc\"\n" +
                        "}",
                ClassWithPropertyAliasOnJsonSetter.class).getProperty())
                .isEqualTo("abc");

        assertThat(objectMapper.readValue(
                "{\n" +
                        "  \"x-property\" : \"abc\"\n" +
                        "}",
                ClassWithPropertyAliasOnJsonSetter.class).getProperty())
                .isEqualTo("abc");

        assertThat(objectMapper.readValue(
                "{\n" +
                        "  \"y-property\" : \"abc\"\n" +
                        "}",
                ClassWithPropertyAliasOnJsonSetter.class).getProperty())
                .isEqualTo("abc");
    }


    // https://stackoverflow.com/questions/62824847/creating-a-custom-jackson-property-naming-strategy
    @AllArgsConstructor
    @NoArgsConstructor
    static class User_diff_name_on_JsonGetter_and_JsonSetter {

        private String firstName;

        @JsonGetter("firstName")
        public String getFirstName(){
            return firstName;
        }

        @JsonSetter("FIRST_NAME")
        public void setFirstName(String firstName)
        {
            this.firstName=firstName;
        }
    }

    @Test
    @SneakyThrows
    void givenFirstNameHasDiffNameOnJsonGetterAndJsonSetter_whenSerialize_thenJsonGetterNameIsUsed() {

        assertThat(objectMapper.writeValueAsString(new User_diff_name_on_JsonGetter_and_JsonSetter("Chris")))
                .isEqualTo(
                        "{\n" +
                                "  \"firstName\" : \"Chris\"\n" +
                                "}"
                );
    }

    @Test
    @SneakyThrows
    void givenFirstNameHasDiffNameOnJsonGetterAndJsonSetter_whenDeserialize_thenJsonSetterNameIsUsed() {

        assertThat(objectMapper.readValue(
                "{\n" +
                        "  \"FIRST_NAME\" : \"Chris\"\n" +
                        "}",
                User_diff_name_on_JsonGetter_and_JsonSetter.class).getFirstName())
                .isEqualTo("Chris");
    }


    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    @Setter
    static class User_JsonAlias_on_field {

        @JsonAlias("FIRST_NAME")
        private String firstName;
    }

    @Test
    @SneakyThrows
    void givenFirstNameWithJsonAlias_whenSerialize_thenOfficialNameIsUsed() {

        assertThat(objectMapper.writeValueAsString(new User_JsonAlias_on_field("Chris")))
                .isEqualTo(
                        "{\n" +
                                "  \"firstName\" : \"Chris\"\n" +
                                "}"
                );
    }

    @Test
    @SneakyThrows
    void givenFirstNameWithJsonAlias_whenDeserialize_thenJsonAliasNameIsUsed() {

        assertThat(objectMapper.readValue(
                "{\n" +
                        "  \"FIRST_NAME\" : \"Chris\"\n" +
                        "}",
                User_JsonAlias_on_field.class).getFirstName())
                .isEqualTo("Chris");
    }


    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    static class User_JsonAlias_on_JsonSetter {

        private String firstName;

        @JsonSetter
        @JsonAlias("FIRST_NAME")
        void setFirstName(String firstName) {
            this.firstName = firstName;
        }
    }

    @Test
    @SneakyThrows
    void givenFirstNameWithJsonAliasOnJsonSetter_whenSerialize_thenOfficialNameIsUsed() {

        assertThat(objectMapper.writeValueAsString(new User_JsonAlias_on_JsonSetter("Chris")))
                .isEqualTo(
                        "{\n" +
                                "  \"firstName\" : \"Chris\"\n" +
                                "}"
                );
    }

    @Test
    @SneakyThrows
    void givenFirstNameWithJsonAliasOnJsonSetter_whenDeserialize_thenJsonAliasNameIsUsed() {

        assertThat(objectMapper.readValue(
                "{\n" +
                        "  \"FIRST_NAME\" : \"Chris\"\n" +
                        "}",
                User_JsonAlias_on_JsonSetter.class).getFirstName())
                .isEqualTo("Chris");
    }

    // @JsonSerialize and @JsonDeserialize on class field specify the customized serializer and deserializer

    @JsonIgnoreProperties({"id"})
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    static class ClassWithIgnoredProperties {

        private int id = -1;

        private String name;
    }

    @Test
    @SneakyThrows
    void JsonIgnoreProperties_specifyPropertiesToIgnoreDuringSerializationAndDeserialization() {

        // ignore "id" during serialization
        assertThat(objectMapper.writeValueAsString(new ClassWithIgnoredProperties(1000, "Ferris")))
                .isEqualTo(
                        "{\n" +
                                "  \"name\" : \"Ferris\"\n" +
                                "}"
                );

        // ignore "id" during deserialization
        assertThat(objectMapper.readValue(
                "{\n" +
                        "  \"id\" : 1000,\n" +
                        "  \"name\" : \"Ferris\"\n" +
                        "}",
                ClassWithIgnoredProperties.class).getId())
                .isEqualTo(-1);
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    static class ClassWithIgnoredPropertiesB {

        @JsonIgnore
        private int id = -1;

        private String name;
    }

    @Test
    @SneakyThrows
    void JsonIgnore_markPropertyToIgnoreDuringSerializationAndDeserialization() {

        // ignore "id" during serialization
        assertThat(objectMapper.writeValueAsString(new ClassWithIgnoredProperties(1000, "Ferris")))
                .isEqualTo(
                        "{\n" +
                                "  \"name\" : \"Ferris\"\n" +
                                "}"
                );

        // ignore "id" during deserialization
        assertThat(objectMapper.readValue(
                "{\n" +
                        "  \"id\" : 1000,\n" +
                        "  \"name\" : \"Ferris\"\n" +
                        "}",
                ClassWithIgnoredProperties.class).getId())
                .isEqualTo(-1);
    }

    // @JsonIgnoreType mark all fields of the annotated type (class) to be ignored

    // @JsonInclude(JsonInclude.Include.NON_NULL)

    // @JsonAutoDetect

    ///////////////////////////////////////////////////////////////////////////
    //  Polymorphic Type Handling
    ///////////////////////////////////////////////////////////////////////////

    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    static class Zoo {

        public Animal animal;
    }

    @JsonTypeInfo(
            use = JsonTypeInfo.Id.NAME
            // include = JsonTypeInfo.As.PROPERTY  // include default to PROPERTY
            // , property = "_type"
    )
    // @JsonSubTypes overwrite the "type" value set in @JsonTypeName on subclass
    @JsonSubTypes({
            @JsonSubTypes.Type(value = Dog.class, name = "dog")
    })
    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    static class Animal {
        String name;
    }

    @JsonTypeName("DOG")
    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode(callSuper = true)
    static class Dog extends Animal {

        int barkVolume;

        Dog(String name, int barkVolume) {

            super(name);
            this.barkVolume = barkVolume;
        }
    }

    @JsonTypeName("CAT")
    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode(callSuper = true)
    static class Cat extends Animal {

        int lives;

        Cat(String name, int lives) {

            super(name);
            this.lives = lives;
        }
    }

    @Test
    @SneakyThrows
    void serializePolymorphicType() {

        assertThat(objectMapper.writeValueAsString(new Zoo(new Dog("Spike", 10))))
                .isEqualTo(
                        "{\n" +
                                "  \"animal\" : {\n" +
                                "    \"@type\" : \"dog\",\n" +
                                "    \"name\" : \"Spike\",\n" +
                                "    \"barkVolume\" : 10\n" +
                                "  }\n" +
                                "}"
                );
        assertThat(objectMapper.writeValueAsString(new Zoo(new Cat("Tom", 9))))
                .isEqualTo(
                        "{\n" +
                                "  \"animal\" : {\n" +
                                "    \"@type\" : \"CAT\",\n" +
                                "    \"name\" : \"Tom\",\n" +
                                "    \"lives\" : 9\n" +
                                "  }\n" +
                                "}"
                );
    }

    @Test
    @SneakyThrows
    void deserializePolymorphicType() {

        assertThat(objectMapper.readValue(
                "{\n" +
                        "  \"animal\" : {\n" +
                        "    \"@type\" : \"dog\",\n" +
                        "    \"name\" : \"Spike\",\n" +
                        "    \"barkVolume\" : 10\n" +
                        "  }\n" +
                        "}",
                Zoo.class))
                .isEqualTo(
                        new Zoo(new Dog("Spike", 10))
                );
    }

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    static class FinalZoo {

        public FinalAnimal animal;
    }

    @JsonTypeInfo(
            use = JsonTypeInfo.Id.NAME
            // include = JsonTypeInfo.As.PROPERTY
            // , property = "_type"
    )
    @JsonSubTypes({
            @JsonSubTypes.Type(value = FinalDog.class, name = "dog"),
            @JsonSubTypes.Type(value = FinalCat.class, name = "cat")
    })
    @Getter
    @Setter
    @RequiredArgsConstructor
    @EqualsAndHashCode
    static class FinalAnimal {
        final String name;
    }

    @Getter
    @Setter
    @EqualsAndHashCode(callSuper = true)
    static class FinalDog extends FinalAnimal {

        int barkVolume;

        @JsonCreator
        FinalDog(@JsonProperty("name") String name, @JsonProperty("barkVolume") int barkVolume) {

            super(name);
            this.barkVolume = barkVolume;
        }
    }

    @Getter
    @Setter
    @EqualsAndHashCode(callSuper = true)
    static class FinalCat extends FinalAnimal {

        int lives;

        FinalCat(String name, int lives) {

            super(name);
            this.lives = lives;
        }
    }

    @Test
    @SneakyThrows
    void serializePolymorphicTypeWithFinalField() {

        assertThat(objectMapper.writeValueAsString(new FinalZoo(new FinalDog("Spike", 10))))
                .isEqualTo(
                        "{\n" +
                                "  \"animal\" : {\n" +
                                "    \"@type\" : \"dog\",\n" +
                                "    \"name\" : \"Spike\",\n" +
                                "    \"barkVolume\" : 10\n" +
                                "  }\n" +
                                "}"
                );
    }

    @Test
    @SneakyThrows
    void deserializePolymorphicTypeWithFinalField_requireJsonCreator_otherwiseInvalidDefinitionException() {

        assertThat(objectMapper.readValue(
                "{\n" +
                        "  \"animal\" : {\n" +
                        "    \"@type\" : \"dog\",\n" +
                        "    \"name\" : \"Spike\",\n" +
                        "    \"barkVolume\" : 10\n" +
                        "  }\n" +
                        "}",
                FinalZoo.class))
                .isEqualTo(
                        new FinalZoo(new FinalDog("Spike", 10))
                );

        assertThatThrownBy(() -> objectMapper.readValue(
                "{\n" +
                        "  \"animal\" : {\n" +
                        "    \"@type\" : \"cat\",\n" +
                        "    \"name\" : \"Tom\",\n" +
                        "    \"lives\" : 9\n" +
                        "  }\n" +
                        "}",
                FinalZoo.class))
                .isInstanceOf(InvalidDefinitionException.class)
                .hasMessageContaining("Cannot construct instance")
                .hasMessageContaining("no Creators, like default construct, exist");
    }

//     @JsonProperty
//     @JsonFormat
//     @JsonUnwrapped
//     @JsonView

    ///////////////////////////////////////////////////////////////////////////
    //  parent / child relationship
    ///////////////////////////////////////////////////////////////////////////

    @JsonIdentityInfo(generator = ObjectIdGenerators.PropertyGenerator.class, property = "id")
    public class ItemWithIdentity {
        public int id;

        public String itemName;

        public UserWithIdentity owner;

        public ItemWithIdentity() {
            super();
        }

        public ItemWithIdentity(final int id, final String itemName, final UserWithIdentity owner) {
            this.id = id;
            this.itemName = itemName;
            this.owner = owner;
        }
    }

    @JsonIdentityInfo(generator = ObjectIdGenerators.PropertyGenerator.class, property = "id")
    public class UserWithIdentity {
        public int id;

        public String name;

        public List<ItemWithIdentity> userItems;

        public UserWithIdentity() {
            super();
        }

        public UserWithIdentity(final int id, final String name) {
            this.id = id;
            this.name = name;
            userItems = new ArrayList<>();
        }

        public void addItem(final ItemWithIdentity item) {
            userItems.add(item);
        }
    }

    @Test
    public void whenSerializingUsingJsonIdentityInfo_thenCorrect()
            throws JsonProcessingException {
        UserWithIdentity user = new UserWithIdentity(1, "John");
        ItemWithIdentity item = new ItemWithIdentity(2, "book", user);
        user.addItem(item);

        String result = objectMapper.writeValueAsString(item);

        log.info(result);
//        assertThat(result, containsString("book"));
//        assertThat(result, containsString("John"));
//        assertThat(result, containsString("userItems"));
    }

    @JsonIdentityInfo(generator = ObjectIdGenerators.PropertyGenerator.class, property = "id")
    @Getter
    @Setter
    @EqualsAndHashCode(onlyExplicitlyIncluded = true)
    static class PostWithId {

        private Long id;

        @EqualsAndHashCode.Include
        private String title;

        @OneToMany(
                mappedBy = "post",
                fetch = FetchType.LAZY,
                cascade = CascadeType.ALL,
                orphanRemoval = true)
        @ToString.Exclude
        private List<PostCommentWithId> postComments = new ArrayList<>();

        void addComment(PostCommentWithId postComment) {

            postComment.setPost(this);
            postComments.add(postComment);
        }

        void removeComment(PostCommentWithId postComment) {

            postComments.remove(postComment);
            postComment.setPost(null);
        }
    }

    @JsonIdentityInfo(
            generator = ObjectIdGenerators.PropertyGenerator.class,
            property = "id"
    )
    @Getter
    @Setter
    @EqualsAndHashCode(onlyExplicitlyIncluded = true)
    static class PostCommentWithId {

        private Long id;

        @EqualsAndHashCode.Include
        private String review;

        @ManyToOne(optional = false, fetch = FetchType.EAGER)
        @JoinColumn(name = "post_id", nullable = false, updatable = false)
        private PostWithId post;
    }

    @Test
    @SneakyThrows
    void JsonIdentityInfoTest() {

        PostWithId post = new PostWithId();
        post.setId(1234L);
        post.setTitle("Arclight of the Point at Infinity");

        PostCommentWithId postComment = new PostCommentWithId();
        postComment.setId(5678L);
        postComment.setReview("Steins gate does exist");

        post.addComment(postComment);

        assertThat(objectMapper.writeValueAsString(post))
                .isEqualTo(
                        "{\n" +
                                "  \"id\" : 1234,\n" +
                                "  \"title\" : \"Arclight of the Point at Infinity\",\n" +
                                "  \"postComments\" : [ {\n" +
                                "    \"id\" : 5678,\n" +
                                "    \"review\" : \"Steins gate does exist\",\n" +
                                "    \"post\" : 1234\n" +
                                "  } ]\n" +
                                "}"
                );

        assertThat(objectMapper.writeValueAsString(postComment))
        .isEqualTo(
                "{\n" +
                        "  \"id\" : 5678,\n" +
                        "  \"review\" : \"Steins gate does exist\",\n" +
                        "  \"post\" : {\n" +
                        "    \"id\" : 1234,\n" +
                        "    \"title\" : \"Arclight of the Point at Infinity\",\n" +
                        "    \"postComments\" : [ 5678 ]\n" +
                        "  }\n" +
                        "}"
        );
    }


    ////////////

    @Getter
    @Setter
    @EqualsAndHashCode(onlyExplicitlyIncluded = true)
    static class Post {

        private Long id;

        @EqualsAndHashCode.Include
        private String title;

        @JsonBackReference
        @OneToMany(
                mappedBy = "post",
                fetch = FetchType.LAZY,
                cascade = CascadeType.ALL,
                orphanRemoval = true)
        @ToString.Exclude
        private List<PostComment> postComments = new ArrayList<>();

        void addComment(PostComment postComment) {

            postComment.setPost(this);
            postComments.add(postComment);
        }

        void removeComment(PostComment postComment) {

            postComments.remove(postComment);
            postComment.setPost(null);
        }
    }

    @Getter
    @Setter
    @EqualsAndHashCode(onlyExplicitlyIncluded = true)
    static class PostComment {

        private Long id;

        @EqualsAndHashCode.Include
        private String review;

        @JsonManagedReference
        @ManyToOne(optional = false, fetch = FetchType.EAGER)
        @JoinColumn(name = "post_id", nullable = false, updatable = false)
        private Post post;
    }

    @Test
    @SneakyThrows
    void JsonManagedReference_and_JsonBackReference_test() {

        Post post = new Post();
        post.setTitle("Arclight of the Point at Infinity");

        PostComment postComment = new PostComment();
        postComment.setReview("Steins gate does exist");

        post.addComment(postComment);

        log.info(objectMapper.writeValueAsString(postComment));
    }


    ///////////////////////////////////////////////////////////////////////////
    // MixIn
    ///////////////////////////////////////////////////////////////////////////
    // MixIn is used when you can't or don't want to add Jackson annotations to be model class

}

