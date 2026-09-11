package org.gbif.parquetexplorer.shim;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

@SpringBootTest
@TestPropertySource(properties = {
    "hdfs.webhdfs-base-url=http://localhost:9870/webhdfs/v1",
    "hdfs.allowed-prefixes=/data",
    "shim.cors-allowed-origin=*"
})
class ParquetExplorerShimApplicationTests {

    @Test
    void contextLoads() {
    }
}
