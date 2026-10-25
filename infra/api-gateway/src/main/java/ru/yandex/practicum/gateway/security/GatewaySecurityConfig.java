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

    private static final String[] RESOURCE_PATHS = {
            "/api/products", "/api/products/**",
            "/api/categories", "/api/categories/**",
            "/api/inventory", "/api/inventory/**"
    };

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
                        .pathMatchers(HttpMethod.GET, RESOURCE_PATHS).permitAll()
                        .pathMatchers(HttpMethod.POST, "/api/orders", "/api/orders/**")
                        .hasRole("USER")
                        .pathMatchers(HttpMethod.GET, "/api/orders/by-email")
                        .hasRole("USER")
                        .pathMatchers(HttpMethod.GET, "/api/orders")
                        .hasRole("ADMIN")
                        .pathMatchers(HttpMethod.GET, "/api/orders/{id}")
                        .hasRole("USER")
                        .pathMatchers(HttpMethod.POST, RESOURCE_PATHS).hasRole("ADMIN")
                        .pathMatchers(HttpMethod.PUT, RESOURCE_PATHS).hasRole("ADMIN")
                        .pathMatchers(HttpMethod.PATCH, RESOURCE_PATHS).hasRole("ADMIN")
                        .pathMatchers(HttpMethod.DELETE, RESOURCE_PATHS).hasRole("ADMIN")
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
        List<UserDetails> users = properties.getUsers().stream()
                .map(user -> User.withUsername(user.getUsername())
                        .password(passwordEncoder.encode(user.getPassword()))
                        .roles(user.getRoles().toArray(String[]::new))
                        .build())
                .toList();

        return new MapReactiveUserDetailsService(users);
    }
}
