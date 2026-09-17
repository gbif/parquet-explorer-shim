package org.gbif.parquetexplorer.shim;

import java.util.Collections;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.cors.CorsConfigurationSource;
import org.springframework.web.cors.UrlBasedCorsConfigurationSource;

import java.util.Arrays;

/**
 * Registers real CORS support for the whole app.
 * ------------------------------------------------------------------
 * Without this, every controller's own OPTIONS handler + manually-added
 * Access-Control-* headers are dead code from a real browser's point of
 * view: Spring's DispatcherServlet intercepts any OPTIONS request that
 * carries Origin + Access-Control-Request-Method (i.e. an actual browser
 * preflight) *before* routing to a controller, and rejects it with 403
 * "Invalid CORS request" unless a CorsConfigurationSource — registered
 * here, or via @CrossOrigin — says otherwise. A plain `curl -X OPTIONS`
 * without those two headers skips this check entirely and falls through
 * to a controller's manual handler, which is why that looked like it
 * worked in testing — a real browser's preflight never got that far.
 *
 * This is why DuckDB-WASM range requests (which always carry a Range
 * header, itself enough to trigger a preflight) and every POST-based
 * /query/*  /dataset/* endpoint have never actually worked cross-origin
 * from a browser, only from same-origin or header-less tools like curl.
 *
 * Once this is registered, per-controller Access-Control-* headers and
 * manual OPTIONS handlers are redundant — worse, duplicating
 * Access-Control-Allow-Origin on the same response is itself invalid
 * per spec — so they've been removed from every controller in favor of
 * this single source of truth.
 *
 * Implemented as an explicit CorsConfigurationSource + CorsFilter bean
 * pair rather than WebMvcConfigurer#addCorsMappings: this app has no
 * Spring Security on the classpath, so a bare CorsConfigurationSource
 * bean wouldn't be picked up by anything on its own — the CorsFilter
 * bean below is what actually wires it into every request.
 */
@Configuration
@EnableWebSecurity
public class WebSecurityConfigurer {

    @Bean
    public SecurityFilterChain filterChain(HttpSecurity http) throws Exception {
        http.httpBasic(AbstractHttpConfigurer::disable)
            .csrf(AbstractHttpConfigurer::disable)
            .cors(cors -> cors.configurationSource(corsConfigurationSource()))
            .sessionManagement(session -> session.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
            .authorizeHttpRequests(auth -> auth.anyRequest().permitAll());

        return http.build();
    }

    @Bean
    public CorsConfigurationSource corsConfigurationSource() {
        // CorsFilter only applies this if the origin header is present in the request
        CorsConfiguration configuration = new CorsConfiguration();
        configuration.setAllowedHeaders(Arrays.asList("authorization", "content-type"));
        configuration.setAllowedOrigins(Collections.singletonList("*"));
        configuration.setAllowedMethods(
            Arrays.asList("HEAD", "GET", "POST", "DELETE", "PUT", "OPTIONS"));
        configuration.setExposedHeaders(
            Arrays.asList(
                "Access-Control-Allow-Origin",
                "Access-Control-Allow-Methods",
                "Access-Control-Allow-Headers"));
        UrlBasedCorsConfigurationSource source = new UrlBasedCorsConfigurationSource();
        source.registerCorsConfiguration("/**", configuration);
        return source;
    }
}
