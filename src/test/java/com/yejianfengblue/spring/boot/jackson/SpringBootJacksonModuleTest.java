package com.yejianfengblue.spring.boot.jackson;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.jackson.JacksonAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

import java.io.IOException;
import java.util.List;

@SpringBootTest
@Slf4j
class SpringBootJacksonModuleTest {

    @Autowired
    ObjectMapper objectMapper;

    @Test
    @SneakyThrows
    void givenModuleBeanWithCustomSerializer_whenSerialize_thenCustomSerializerIsUsed() {

        MyRepresentationModel model = new MyRepresentationModel("xyz", "www.google.com");

        log.info(objectMapper.writeValueAsString(model));
    }

    @TestConfiguration
    static class config {

        @Bean
        Module myModule() {
            return new MyRepresentationModelModule();
        }
    }

    static class MyRepresentationModel<T> {

        private final T content;

        private final List<String> links;

        MyRepresentationModel(T content, String... links) {

            this.content = content;
            this.links = List.of(links);
        }

        T getContent() {
            return this.content;
        }

        List<String> getLinks() {
            return this.links;
        }
    }

    static class MyRepresentationModelSerializer extends StdSerializer<MyRepresentationModel> {

        MyRepresentationModelSerializer() {
            super(MyRepresentationModel.class);
        }

        @Override
        public void serialize(MyRepresentationModel value, JsonGenerator gen, SerializerProvider provider) throws IOException {

            gen.writeStartObject();  // {

            if (value != null) {
                gen.writeStringField("content", value.getContent().toString());
            } else {
                gen.writeNull();
            }

            gen.writeEndObject();  // }

        }
    }

    /**
     * Module will be picked up by
     * {@link JacksonAutoConfiguration.Jackson2ObjectMapperBuilderCustomizerConfiguration.StandardJackson2ObjectMapperBuilderCustomizer#configureModules(org.springframework.http.converter.json.Jackson2ObjectMapperBuilder)}
     */
    static class MyRepresentationModelModule extends SimpleModule {

        MyRepresentationModelModule() {

            addSerializer(MyRepresentationModel.class, new MyRepresentationModelSerializer());
        }
    }

}
