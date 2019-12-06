package com.yejianfengblue.spring.boot.validation;

import lombok.Data;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.math.BigDecimal;
import java.util.Optional;
import java.util.Set;
import javax.validation.ConstraintViolation;
import javax.validation.Validator;
import javax.validation.constraints.Digits;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import javax.validation.constraints.Size;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases to JSR-303 (bean validation 1.0), JSR-349 (bean validation 1.1) and JSR-380 (bean validation 2.0)
 *
 * @author yejianfengblue
 */
@SpringBootTest
class BeanValidationTest {

    @Autowired
    private Validator validator;

    @Data
    private static class ValidatedBean {

        @NotNull
        private String notNullString;

        @Size(max = 10)
        private String maxSize10String;

        @Digits(integer = 3, fraction = 2)
        private BigDecimal int3Fra2Decimal;

        @Pattern(regexp = "[IO]", message = "is not 'I' or 'O'")
        private String inOutInd;
    }

    @Test
    @DisplayName("@NotNull positive test")
    void givenFieldAnnotatedWithNotNull_WhenAssignNotNullAndValidate_ThenNoError() {

        // given
        ValidatedBean validatedBean = new ValidatedBean();
        validatedBean.setNotNullString("not null");

        // when
        Set<ConstraintViolation<ValidatedBean>> violations = validator.validate(validatedBean);

        // then
        assertEquals(0, violations.stream()
                .filter(v -> v.getPropertyPath().toString().equals("notNullString"))
                .count());
    }

    @Test
    @DisplayName("@NotNull negative test")
    void givenFieldAnnotatedWithNotNull_WhenAssignNullAndValidate_ThenErrorMessageIsMustNotBeNull() {

        // given
        ValidatedBean validatedBean = new ValidatedBean();
        validatedBean.setNotNullString(null);

        // when
        Set<ConstraintViolation<ValidatedBean>> violations = validator.validate(validatedBean);

        // then
        Optional<ConstraintViolation<ValidatedBean>> notNullStringViolation = violations.stream()
                .filter(v -> v.getPropertyPath().toString().equals("notNullString"))
                .findFirst();
        assertTrue(notNullStringViolation.isPresent());
        assertEquals("notNullString", notNullStringViolation.get().getPropertyPath().toString());
        assertEquals("must not be null", notNullStringViolation.get().getMessage());
    }

    @Test
    @DisplayName("@Size positive test")
    void givenStringFieldAnnotatedWithMaxSize10_WhenAssign10CharsAndValidate_ThenNoError() {

        // given
        ValidatedBean validatedBean = new ValidatedBean();
        validatedBean.setMaxSize10String("0123456789");

        // when
        Set<ConstraintViolation<ValidatedBean>> violations = validator.validate(validatedBean);

        // then
        assertEquals(0, violations.stream()
                .filter(v -> v.getPropertyPath().toString().equals("maxSize10String"))
                .count());
    }

    @Test
    @DisplayName("@Size negative test")
    void givenStringFieldAnnotatedWithMaxSize10_WhenAssign11CharsAndValidate_ThenErrorMessageIsSizeMustBeBetween0And10() {

        // given
        ValidatedBean validatedBean = new ValidatedBean();
        validatedBean.setMaxSize10String("0123456789a");

        // when
        Set<ConstraintViolation<ValidatedBean>> violations = validator.validate(validatedBean);

        // then
        Optional<ConstraintViolation<ValidatedBean>> maxSize10StringViolation = violations.stream()
                .filter(v -> v.getPropertyPath().toString().equals("maxSize10String"))
                .findFirst();
        assertTrue(maxSize10StringViolation.isPresent());
        assertEquals("maxSize10String", maxSize10StringViolation.get().getPropertyPath().toString());
        assertEquals("size must be between 0 and 10", maxSize10StringViolation.get().getMessage());
    }

    @Test
    @DisplayName("@Digits positive test")
    void givenBigDecimalFieldAnnotatedWithDigitsInteger3AndFraction2_WhenAssign3IntegerAnd2FractionAndValidate_ThenNoError() {

        // given
        ValidatedBean validatedBean = new ValidatedBean();
        validatedBean.setInt3Fra2Decimal(new BigDecimal("123.45"));

        // when
        Set<ConstraintViolation<ValidatedBean>> violations = validator.validate(validatedBean);

        // then
        assertEquals(0, violations.stream()
                .filter(v -> v.getPropertyPath().toString().equals("int3Fra2Decimal"))
                .count());
    }

    @Test
    @DisplayName("@Digits.integer negative test")
    void givenBigDecimalFieldAnnotatedWithDigitsInteger3AndFraction2_WhenAssign4IntegerAndValidate_ThenErrorMessageIsNumericValueOutOfBounds() {

        // given
        ValidatedBean validatedBean = new ValidatedBean();
        validatedBean.setInt3Fra2Decimal(new BigDecimal("1234.56"));

        // when
        Set<ConstraintViolation<ValidatedBean>> violations = validator.validate(validatedBean);

        // then
        Optional<ConstraintViolation<ValidatedBean>> int3Fra2DecimalViolation = violations.stream()
                .filter(v -> v.getPropertyPath().toString().equals("int3Fra2Decimal"))
                .findFirst();
        assertTrue(int3Fra2DecimalViolation.isPresent());
        assertEquals("int3Fra2Decimal", int3Fra2DecimalViolation.get().getPropertyPath().toString());
        assertEquals("numeric value out of bounds (<3 digits>.<2 digits> expected)", int3Fra2DecimalViolation.get().getMessage());
    }

    @Test
    @DisplayName("@Digits.integer negative test")
    void givenBigDecimalFieldAnnotatedWithDigitsInteger3AndFraction2_WhenAssign3FractionAndValidate_ThenErrorMessageIsNumericValueOutOfBounds() {

        // given
        ValidatedBean validatedBean = new ValidatedBean();
        validatedBean.setInt3Fra2Decimal(new BigDecimal("123.456"));

        // when
        Set<ConstraintViolation<ValidatedBean>> violations = validator.validate(validatedBean);

        // then
        Optional<ConstraintViolation<ValidatedBean>> int3Fra2DecimalViolation = violations.stream()
                .filter(v -> v.getPropertyPath().toString().equals("int3Fra2Decimal"))
                .findFirst();
        assertTrue(int3Fra2DecimalViolation.isPresent());
        assertEquals("int3Fra2Decimal", int3Fra2DecimalViolation.get().getPropertyPath().toString());
        assertEquals("numeric value out of bounds (<3 digits>.<2 digits> expected)", int3Fra2DecimalViolation.get().getMessage());
    }

    @Test
    @DisplayName("@Pattern positive test")
    void givenStringFieldAnnotatedWithPatternAndCustomizedMessage_WhenAssignMatchedStringAndValidate_ThenNoError() {

        // given
        ValidatedBean validatedBean = new ValidatedBean();
        validatedBean.setInOutInd("I");

        // when
        Set<ConstraintViolation<ValidatedBean>> violations = validator.validate(validatedBean);

        // then
        assertEquals(0, violations.stream()
                .filter(v -> v.getPropertyPath().toString().equals("inOutInd"))
                .count());
    }

    @Test
    @DisplayName("@Pattern negative test")
    void givenStringFieldAnnotatedWithPatternAndCustomizedMessage_WhenAssignNotMatchedStringAndValidate_ThenErrorMessageIsThatCustomizedMessage() {

        // given
        ValidatedBean validatedBean = new ValidatedBean();
        validatedBean.setInOutInd("A");

        // when
        Set<ConstraintViolation<ValidatedBean>> violations = validator.validate(validatedBean);

        // then
        Optional<ConstraintViolation<ValidatedBean>> inOutIndViolation = violations.stream()
                .filter(v -> v.getPropertyPath().toString().equals("inOutInd"))
                .findFirst();
        assertTrue(inOutIndViolation.isPresent());
        assertEquals("inOutInd", inOutIndViolation.get().getPropertyPath().toString());
        assertEquals("is not 'I' or 'O'", inOutIndViolation.get().getMessage());
    }
}
