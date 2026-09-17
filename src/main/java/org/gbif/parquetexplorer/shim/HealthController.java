package org.gbif.parquetexplorer.shim;

import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Plain GET health check on the main server port, for Nagios (or anything
 * else) that only reaches this service through Varnish — /actuator/health
 * runs on the separate management port, which Varnish's backend definition
 * for this service doesn't point at.
 */
@RestController
public class HealthController {

    @GetMapping("/health")
    public Map<String, String> health() {
        return Map.of("status", "UP");
    }
}
