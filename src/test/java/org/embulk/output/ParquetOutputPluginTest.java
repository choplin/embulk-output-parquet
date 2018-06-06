package org.embulk.output;

import org.apache.hadoop.conf.Configuration;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.spi.Exec;
import org.junit.Rule;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ParquetOutputPluginTest
{
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    @Test
    public void checkDefaultValues()
    {
        ConfigSource config = Exec.newConfigSource()
                .set("path_prefix", "test");

        ParquetOutputPlugin.PluginTask task = config.loadConfig(ParquetOutputPlugin.PluginTask.class);
        assertEquals(".parquet", task.getFileNameExtension());
        assertEquals(".%03d", task.getSequenceFormat());
        assertEquals(134217728, task.getBlockSize());
        assertEquals(1048576, task.getPageSize());
        assertEquals("UNCOMPRESSED", task.getCompressionCodec());
        assertFalse(task.getOverwrite());
    }

    @Test(expected = ConfigException.class)
    public void checkColumnsRequired()
    {
        ConfigSource config = Exec.newConfigSource();

        config.loadConfig(ParquetOutputPlugin.PluginTask.class);
    }

    @Test
    public void checkExtraConfigurations()
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException
    {
        ConfigSource map = Exec.newConfigSource()
                .set("foo", "bar");

        ConfigSource config = Exec.newConfigSource()
                .set("path_prefix", "test")
                .setNested("extra_configurations", map);

        ParquetOutputPlugin.PluginTask task = config.loadConfig(ParquetOutputPlugin.PluginTask.class);

        Map<String, String> extra = task.getExtraConfigurations();
        assertTrue(extra.containsKey("foo"));
        assertEquals("bar", extra.get("foo"));

        ParquetOutputPlugin plugin = new ParquetOutputPlugin();
        Method method = ParquetOutputPlugin.class.getDeclaredMethod("createConfiguration", Map.class, List.class);
        method.setAccessible(true);
        Configuration conf = (Configuration) method.invoke(plugin, extra, new ArrayList<String>());
        assertEquals("bar", conf.get("foo"));
    }
}
