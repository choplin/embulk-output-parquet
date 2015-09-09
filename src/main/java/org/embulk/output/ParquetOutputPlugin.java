package org.embulk.output;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.embulk.config.TaskReport;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.Column;
import org.embulk.spi.Exec;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.TransactionalPageOutput;
import org.embulk.spi.time.TimestampFormatter;
import org.embulk.spi.type.TimestampType;
import parquet.hadoop.ParquetWriter;
import parquet.hadoop.api.WriteSupport;
import parquet.hadoop.metadata.CompressionCodecName;

@SuppressWarnings("unused")
public class ParquetOutputPlugin
        implements OutputPlugin
{
    public interface PluginTask
            extends Task, TimestampFormatter.FormatterTask
    {
        @Config("path_prefix")
        public String getPathPrefix();

        @Config("file_ext")
        @ConfigDefault("\".parquet\"")
        public String getFileNameExtension();

        @Config("sequence_format")
        @ConfigDefault("\".%03d\"")
        public String getSequenceFormat();

        @Config("block_size")
        @ConfigDefault("134217728") // 128M
        public int getBlockSize();

        @Config("page_size")
        @ConfigDefault("1048576") // 1M
        public int getPageSize();

        @Config("compression_codec")
        @ConfigDefault("\"UNCOMPRESSED\"")
        public String getCompressionCodec();
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

        final String pathPrefix = task.getPathPrefix();
        final String pathSuffix = task.getFileNameExtension();
        final String sequenceFormat = task.getSequenceFormat();
        final CompressionCodecName codec = CompressionCodecName.valueOf(task.getCompressionCodec());
        final int blockSize = task.getBlockSize();
        final int pageSize = task.getPageSize();

        final String path = pathPrefix + String.format(sequenceFormat, processorIndex) + pathSuffix;

        final PageReader reader = new PageReader(schema);

        final Map<Integer, TimestampFormatter> timestampFormatters = newTimestampFormatters(task, schema);
        final EmbulkWriteSupport writeSupport = new EmbulkWriteSupport(schema, timestampFormatters);
        ParquetWriter<PageReader> writer = createParquetWriter(new Path(path), writeSupport, codec, blockSize, pageSize);

        return new ParquetTransactionalPageOutput(reader, writer);
    }

    private Map<Integer, TimestampFormatter> newTimestampFormatters(
            TimestampFormatter.FormatterTask task, Schema schema)
    {
        ImmutableMap.Builder<Integer, TimestampFormatter> builder = new ImmutableBiMap.Builder<>();
        for (Column column : schema.getColumns()) {
            if (column.getType() instanceof TimestampType) {
                TimestampType tt = (TimestampType) column.getType();
                builder.put(column.getIndex(), new TimestampFormatter(tt.getFormat(), task));
            }
        }
        return builder.build();
    }

    private <T> ParquetWriter<T> createParquetWriter(Path path, WriteSupport<T> writeSupport, CompressionCodecName codec, int blockSize, int pageSize) {
        ParquetWriter<T> writer = null;

        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", LocalFileSystem.class.getName());
        conf.setClassLoader(this.getClass().getClassLoader());

        try {
            writer = new ParquetWriter<>(
                    path,
                    writeSupport,
                    codec,
                    blockSize,
                    pageSize,
                    pageSize,
                    ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED,
                    ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED,
                    ParquetWriter.DEFAULT_WRITER_VERSION,
                    conf);
        } catch (IOException e) {
            Throwables.propagate(e);
        }
        return writer;
    }

    class ParquetTransactionalPageOutput implements TransactionalPageOutput {
        private PageReader reader;
        private ParquetWriter<PageReader> writer;

        public ParquetTransactionalPageOutput(PageReader reader, ParquetWriter<PageReader> writer) {
            this.reader = reader;
            this.writer = writer;
        }

        @Override
        public void add(Page page) {
            try {
                reader.setPage(page);
                while (reader.nextRecord()) {
                    writer.write(reader);
                }
            } catch(IOException e) {
                Throwables.propagate(e);
            }
        }

        @Override
        public void finish() {
            try {
                writer.close();
                writer = null;
            } catch (IOException e) {
                Throwables.propagate(e);
            }
        }

        @Override
        public void close() {
            //TODO
        }

        @Override
        public void abort() {
            //TODO
        }

        @Override
        public TaskReport commit() {
            return Exec.newTaskReport();
            //TODO
        }
    }
}
