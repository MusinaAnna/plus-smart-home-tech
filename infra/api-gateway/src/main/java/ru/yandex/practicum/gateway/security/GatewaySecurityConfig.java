package ru.yandex.practicum.gateway.security;

import org.springframework.cloud.gateway.config.GlobalCorsProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.reactive.EnableWebFluxSecurity;
import org.springframework.security.config.web.server.ServerHttpSecurity;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.core.userdetails.MapReactiveUserDetailsService;
import org.springframework.security.web.server.SecurityWebFilterChain;
import org.springframework.security.web.server.context.NoOpServerSecurityContextRepository;
import org.springframework.web.cors.reactive.CorsConfigurationSource;
import org.springframework.web.cors.reactive.UrlBasedCorsConfigurationSource;

import java.util.List;

@Configuration
@EnableWebFluxSecurity
@EnableConfigurationProperties(GatewaySecurityProperties.class)
public class GatewaySecurityConfig {

    @Bean
    public SecurityWebFilterChain securityWebFilterChain(ServerHttpSecurity http) {
        return http
                .csrf(ServerHttpSecurity.CsrfSpec::disable)
                .cors(Customizer.withDefaults())
                .formLogin(ServerHttpSecurity.FormLoginSpec::disable)
                .logout(ServerHttpSecurity.LogoutSpec::disable)
                .httpBasic(Customizer.withDefaults())
                .securityContextRepository(NoOpServerSecurityContextRepository.getInstance())
                .authorizeExchange(exchanges -> exchanges
                        .pathMatchers(HttpMethod.OPTIONS, "/**").permitAll()
                        .pathMatchers(
                                "/swagger-ui.html",
                                "/swagger-ui/**",
                                "/swagger-resources/**",
                                "/v3/api-docs",
                                "/v3/api-docs/**",
                                "/webjars/**"
                        ).permitAll()
                        .pathMatchers(HttpMethod.GET,
                                "/api/products", "/api/products/**",
                                "/api/categories", "/api/categories/**",
                                "/api/inventory", "/api/inventory/**"
                        ).permitAll()
                        .pathMatchers(HttpMethod.POST, "/api/orders", "/api/orders/**")
                        .hasRole("USER")
                        .pathMatchers(HttpMethod.GET, "/api/orders/by-email")
                        .hasRole("USER")
                        .pathMatchers(HttpMethod.GET, "/api/orders")
                        .hasRole("ADMIN")
                        .pathMatchers(HttpMethod.GET, "/api/orders/{id}")
                        .hasRole("USER")
                        .pathMatchers(HttpMethod.POST,
                                "/api/products", "/api/products/**",
                                "/api/categories", "/api/categories/**",
                                "/api/inventory", "/api/inventory/**"
                        ).hasRole("ADMIN")
                        .pathMatchers(HttpMethod.PUT,
                                "/api/products", "/api/products/**",
                                "/api/categories", "/api/categories/**",
                                "/api/inventory", "/api/inventory/**"
                        ).hasRole("ADMIN")
                        .pathMatchers(HttpMethod.PATCH,
                                "/api/products", "/api/products/**",
                                "/api/categories", "/api/categories/**",
                                "/api/inventory", "/api/inventory/**"
                        ).hasRole("ADMIN")
                        .pathMatchers(HttpMethod.DELETE,
                                "/api/products", "/api/products/**",
                                "/api/categories", "/api/categories/**",
                                "/api/inventory", "/api/inventory/**"
                        ).hasRole("ADMIN")
                        .anyExchange().denyAll()
                )
                .build();
    }

    @Bean
    public CorsConfigurationSource corsConfigurationSource(GlobalCorsProperties gatewayCorsProperties) {
        UrlBasedCorsConfigurationSource source = new UrlBasedCorsConfigurationSource();
        gatewayCorsProperties.getCorsConfigurations()
                .forEach(source::registerCorsConfiguration);
        return source;
    }

    @Bean
    public PasswordEncoder passwordEncoder() {
        return new BCryptPasswordEncoder();
    }

    @Bean
    public MapReactiveUserDetailsService reactiveUserDetailsService(
            GatewaySecurityProperties properties,
            PasswordEncoder passwordEncoder
    ) {
        if (properties.getUsers() == null || properties.getUsers().isEmpty()) {
            throw new IllegalStateException("Gateway users are missing in app.security.users");
        }

        List<UserDetails> users = properties.getUsers().stream()
                .map(user -> {
                    if (user.getUsername() == null || user.getUsername().isBlank()
                            || user.getPassword() == null || user.getPassword().isBlank()
                            || user.getRoles() == null || user.getRoles().isEmpty()) {
                        throw new IllegalStateException("Invalid Gateway user in app.security.users");
                    }
                    return User.withUsername(user.getUsername())
                            .password(passwordEncoder.encode(user.getPassword()))
                            .roles(user.getRoles().toArray(String[]::new))
                            .build();
                })
                .toList();

        return new MapReactiveUserDetailsService(users);
    }
}
