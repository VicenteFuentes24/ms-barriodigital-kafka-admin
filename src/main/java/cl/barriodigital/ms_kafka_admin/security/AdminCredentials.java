package cl.barriodigital.ms_kafka_admin.security;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** Deliberately not a record: no generated toString containing the password. */
@ConfigurationProperties("app.admin-auth")
public class AdminCredentials {
    private String username;
    private String password;
    public String getUsername() { return username; }
    public void setUsername(String username) { this.username = username; }
    public String getPassword() { return password; }
    public void setPassword(String password) { this.password = password; }
    public void requireConfigured() {
        if (username == null || username.isBlank() || password == null || password.isBlank()
                || password.length() < 16 || password.getBytes(java.nio.charset.StandardCharsets.UTF_8).length > 72) {
            throw new IllegalStateException("Configure KAFKA_ADMIN_USERNAME and a KAFKA_ADMIN_PASSWORD of at least 16 characters and at most 72 UTF-8 bytes");
        }
    }
}
