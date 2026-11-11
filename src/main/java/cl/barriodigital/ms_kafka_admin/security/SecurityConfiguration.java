package cl.barriodigital.ms_kafka_admin.security;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.*;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.core.userdetails.*;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.provisioning.InMemoryUserDetailsManager;
import org.springframework.security.web.SecurityFilterChain;

@Configuration
@EnableConfigurationProperties(AdminCredentials.class)
public class SecurityConfiguration {
    @Bean public UserDetailsService adminUsers(AdminCredentials credentials) {
        credentials.requireConfigured();
        return new InMemoryUserDetailsManager(User.withUsername(credentials.getUsername())
                .password("{bcrypt}" + new BCryptPasswordEncoder().encode(credentials.getPassword())).roles("ADMIN").build());
    }
    @Bean SecurityFilterChain apiSecurity(HttpSecurity http) throws Exception {
        return http.authorizeHttpRequests(auth -> auth
                        .requestMatchers("/actuator/health", "/actuator/health/**").permitAll()
                        .requestMatchers("/api/kafka/admin/**").hasRole("ADMIN")
                        .anyRequest().denyAll())
                .httpBasic(basic -> basic.authenticationEntryPoint((request,response,error) -> {
                    response.setHeader("WWW-Authenticate", "Basic realm=\"Kafka administration\"");
                    response.setStatus(401); response.setContentType("application/json");
                    response.getWriter().write("{\"status\":401,\"code\":\"UNAUTHENTICATED\",\"message\":\"Administrative authentication required\"}");
                }))
                .exceptionHandling(errors -> errors.accessDeniedHandler((request,response,error) -> {
                    response.setStatus(403); response.setContentType("application/json");
                    response.getWriter().write("{\"status\":403,\"code\":\"FORBIDDEN\",\"message\":\"Administrator role and valid CSRF token required\"}");
                }))
                .csrf(Customizer.withDefaults())
                .requestCache(cache -> cache.disable())
                .build();
    }
}
