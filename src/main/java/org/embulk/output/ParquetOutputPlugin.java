package org.embulk.output;

import com.google.common.base.Throwables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.Exec;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.TransactionalPageOutput;
import org.embulk.spi.time.TimestampFormatter;
import org.embulk.spi.util.Timestamps;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@SuppressWarnings("unused")
public class ParquetOutputPlugin
        implements OutputPlugin
{
    public interface PluginTask
            extends Task, TimestampFormatter.Task
    {
        @Config("path_prefix")
        String getPathPrefix();

        @Config("file_ext")
        @ConfigDefault("\".parquet\"")
        String getFileNameExtension();

        @Config("sequence_format")
        @ConfigDefault("\".%03d\"")
        String getSequenceFormat();

        @Config("block_size")
        @ConfigDefault("134217728")
            // 128M
        int getBlockSize();

        @Config("page_size")
        @ConfigDefault("1048576")
            // 1M
        int getPageSize();

        @Config("compression_codec")
        @ConfigDefault("\"UNCOMPRESSED\"")
        String getCompressionCodec();

        @Config("column_options")
        @ConfigDefault("{}")
        Map<String, TimestampColumnOption> getColumnOptions();

        @Config("extra_configurations")
        @ConfigDefault("{}")
        Map<String, String> getExtraConfigurations();

        @Config("overwrite")
        @ConfigDefault("false")
        boolean getOverwrite();

        @Config("addUTF8")
        @ConfigDefault("false")
        boolean getAddUTF8();
    }

    public interface TimestampColumnOption
            extends Task, TimestampFormatter.TimestampColumnOption
    {
    }

    public ConfigDiff transaction(ConfigSource config,
            Schema schema, int processorCount,
            OutputPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        //TODO

        control.run(task.dump());
        return Exec.newConfigDiff();
    }

    public ConfigDiff resume(TaskSource taskSource,
            Schema schema, int processorCount,
            OutputPlugin.Control control)
    {
        throw new UnsupportedOperationException("parquet output plugin does not support resuming");
    }

    public void cleanup(TaskSource taskSource,
            Schema schema, int processorCount,
            List<TaskReport> successTaskReports)
    {
        //TODO
    }

    public TransactionalPageOutput open(TaskSource taskSource, final Schema schema, int processorIndex)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);

        final PageReader reader = new PageReader(schema);
        final ParquetWriter<PageReader> writer = createWriter(task, schema, processorIndex);

        return new ParquetTransactionalPageOutput(reader, writer);
    }

    private String buildPath(PluginTask task, int processorIndex)
    {
        final String pathPrefix = task.getPathPrefix();
        final String pathSuffix = task.getFileNameExtension();
        final String sequenceFormat = task.getSequenceFormat();
        return pathPrefix + String.format(sequenceFormat, processorIndex) + pathSuffix;
    }

    private ParquetWriter<PageReader> createWriter(PluginTask task, Schema schema, int processorIndex)
    {
        final TimestampFormatter[] timestampFormatters = Timestamps.newTimestampColumnFormatters(task, schema, task.getColumnOptions());
        final boolean addUTF8 = task.getAddUTF8();

        final Path path = new Path(buildPath(task, processorIndex));
        final CompressionCodecName codec = CompressionCodecName.valueOf(task.getCompressionCodec());
        final int blockSize = task.getBlockSize();
        final int pageSize = task.getPageSize();
        final Configuration conf = createConfiguration(task.getExtraConfigurations());
        final boolean overwrite = task.getOverwrite();

        ParquetWriter<PageReader> writer = null;
        try {
            EmbulkWriterBuilder builder = new EmbulkWriterBuilder(path, schema, timestampFormatters, addUTF8)
                    .withCompressionCodec(codec)
                    .withRowGroupSize(blockSize)
                    .withPageSize(pageSize)
                    .withDictionaryPageSize(pageSize)
                    .withConf(conf);

            if (overwrite) {
                builder.withWriteMode(ParquetFileWriter.Mode.OVERWRITE);
            }

            writer = builder.build();
        }
        catch (IOException e) {
            Throwables.propagate(e);
        }
        return writer;
    }

    private Configuration createConfiguration(Map<String, String> extra)
    {
        Configuration conf = new Configuration();

        // Default values
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", LocalFileSystem.class.getName());

        // Optional values
        for (Map.Entry<String, String> entry : extra.entrySet()) {
            conf.set(entry.getKey(), entry.getValue());
        }

        conf.setClassLoader(this.getClass().getClassLoader());

        return conf;
    }

    class ParquetTransactionalPageOutput
            implements TransactionalPageOutput
    {
        private PageReader reader;
        private ParquetWriter<PageReader> writer;

        public ParquetTransactionalPageOutput(PageReader reader, ParquetWriter<PageReader> writer)
        {
            this.reader = reader;
            this.writer = writer;
        }

        @Override
        public void add(Page page)
        {
            try {
                reader.setPage(page);
                while (reader.nextRecord()) {
                    writer.write(reader);
                }
            }
            catch (IOException e) {
                Throwables.propagate(e);
            }
        }

        @Override
        public void finish()
        {
            try {
                writer.close();
                writer = null;
            }
            catch (IOException e) {
                Throwables.propagate(e);
            }
        }

        @Override
        public void close()
        {
            //TODO
        }

        @Override
        public void abort()
        {
            //TODO
        }

        @Override
        public TaskReport commit()
        {
            return Exec.newTaskReport();
            //TODO
        }
    }
}
