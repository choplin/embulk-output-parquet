package org.embulk.output;

import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.spi.Exec;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.*;

public class ParquetOutputPluginTest {
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    @Test
    public void checkDefaultValues() {
        ConfigSource config = Exec.newConfigSource()
                .set("path_prefix", "test");

        ParquetOutputPlugin.PluginTask task = config.loadConfig(ParquetOutputPlugin.PluginTask.class);
        assertEquals(".parquet", task.getFileNameExtension());
        assertEquals(".%03d", task.getSequenceFormat());
        assertEquals(134217728, task.getBlockSize());
        assertEquals(1048576, task.getPageSize());
        assertEquals("UNCOMPRESSED", task.getCompressionCodec());
    }

    @Test(expected = ConfigException.class)
    public void checkColumnsRequired() {
        ConfigSource config = Exec.newConfigSource();

        config.loadConfig(ParquetOutputPlugin.PluginTask.class);
    }


}
