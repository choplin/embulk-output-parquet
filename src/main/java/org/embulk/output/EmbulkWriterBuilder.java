package org.embulk.output;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.time.TimestampFormatter;

public class EmbulkWriterBuilder
        extends ParquetWriter.Builder<PageReader, EmbulkWriterBuilder>
{
    final Schema schema;
    final TimestampFormatter[] timestampFormatters;

    public EmbulkWriterBuilder(Path file, Schema schema, TimestampFormatter[] timestampFormatters)
    {
        super(file);
        this.schema = schema;
        this.timestampFormatters = timestampFormatters;
    }

    @Override
    protected EmbulkWriterBuilder self()
    {
        return this;
    }

    @Override
    protected WriteSupport<PageReader> getWriteSupport(Configuration conf)
    {
        return new EmbulkWriteSupport(schema, timestampFormatters);
    }
}
