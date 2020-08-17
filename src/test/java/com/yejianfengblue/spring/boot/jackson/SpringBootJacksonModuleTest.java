package com.yejianfengblue.spring.boot.jackson;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.jackson.JacksonAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.hateoas.RepresentationModel;

import java.io.IOException;

@SpringBootTest
@Slf4j
class SpringBootJacksonModuleTest {

    @Autowired
    ObjectMapper objectMapper;

    @Test
    @SneakyThrows
    void givenModuleBeanWithCustomSerializer_whenSerialize_thenCustomSerializerIsUsed() {

        UserModel userModel = new UserModel("xyz", "XYZ");

        log.info(objectMapper.writeValueAsString(userModel));
    }

    @TestConfiguration
    static class JacksonCustomization {

        @Bean
        Module userModelModule() {
            return new UserModelModule();
        }
    }

    @Data
    @AllArgsConstructor
    static class UserModel extends RepresentationModel<UserModel> {

        String username;

        String password;
    }

    static class UserModelSerializer extends StdSerializer<UserModel> {

        UserModelSerializer() {
            super(UserModel.class);
        }

        @Override
        public void serialize(UserModel value, JsonGenerator gen, SerializerProvider provider) throws IOException {

            gen.writeStartObject();  // {

            if (value != null) {
                gen.writeStringField("username", value.getUsername());
                gen.writeStringField("password", value.getPassword());
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
    static class UserModelModule extends SimpleModule {

        UserModelModule() {

            addSerializer(UserModel.class, new UserModelSerializer());
        }
    }

}
