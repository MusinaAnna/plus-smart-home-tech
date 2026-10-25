package ru.yandex.practicum.gateway.security;

import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class GatewaySecurityPropertiesTest {

    private static ValidatorFactory validatorFactory;
    private static Validator validator;

    @BeforeAll
    static void setUp() {
        validatorFactory = Validation.buildDefaultValidatorFactory();
        validator = validatorFactory.getValidator();
    }

    @AfterAll
    static void tearDown() {
        validatorFactory.close();
    }

    @Test
    void validUserIsAccepted() {
        GatewaySecurityProperties properties = propertiesWithUser("ivan", "ivan", List.of("USER"));

        assertThat(validator.validate(properties)).isEmpty();
    }

    @Test
    void emptyUsersListIsRejected() {
        GatewaySecurityProperties properties = new GatewaySecurityProperties();

        assertThat(validator.validate(properties)).isNotEmpty();
    }

    @Test
    void blankUsernameIsRejected() {
        GatewaySecurityProperties properties = propertiesWithUser(" ", "ivan", List.of("USER"));

        assertThat(validator.validate(properties)).isNotEmpty();
    }

    @Test
    void blankPasswordIsRejected() {
        GatewaySecurityProperties properties = propertiesWithUser("ivan", " ", List.of("USER"));

        assertThat(validator.validate(properties)).isNotEmpty();
    }

    @Test
    void emptyRolesListIsRejected() {
        GatewaySecurityProperties properties = propertiesWithUser("ivan", "ivan", List.of());

        assertThat(validator.validate(properties)).isNotEmpty();
    }

    @Test
    void blankRoleIsRejected() {
        GatewaySecurityProperties properties = propertiesWithUser("ivan", "ivan", List.of(" "));

        assertThat(validator.validate(properties)).isNotEmpty();
    }

    private static GatewaySecurityProperties propertiesWithUser(
            String username, String password, List<String> roles) {
        GatewaySecurityProperties.SecurityUser user = new GatewaySecurityProperties.SecurityUser();
        user.setUsername(username);
        user.setPassword(password);
        user.setRoles(roles);

        GatewaySecurityProperties properties = new GatewaySecurityProperties();
        properties.setUsers(List.of(user));
        return properties;
    }
}
