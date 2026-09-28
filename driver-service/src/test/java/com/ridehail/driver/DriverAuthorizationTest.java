package com.ridehail.driver;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.security.Principal;
import org.junit.jupiter.api.Test;
import org.springframework.web.server.ResponseStatusException;

class DriverAuthorizationTest {
    @Test void permitsOnlyTheDriverMatchingTheTokenSubject() {
        Principal ownIdentity = () -> "d1";
        assertThatCode(() -> DriverController.requireOwnId("d1", ownIdentity)).doesNotThrowAnyException();
        assertThatThrownBy(() -> DriverController.requireOwnId("d2", ownIdentity))
                .isInstanceOf(ResponseStatusException.class)
                .hasMessageContaining("identity does not match");
    }
}
