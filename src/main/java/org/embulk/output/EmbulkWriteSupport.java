package org.embulk.output;

import org.apache.hadoop.conf.Configuration;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.time.TimestampFormatter;
import parquet.hadoop.api.WriteSupport;
import parquet.io.api.Binary;
import parquet.io.api.RecordConsumer;
import parquet.schema.MessageType;
import parquet.schema.PrimitiveType;
import parquet.schema.PrimitiveType.PrimitiveTypeName;
import parquet.schema.Type;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EmbulkWriteSupport extends WriteSupport<PageReader> {
    final Schema schema;
    RecordConsumer consumer;
    WriteContext writeContext;
    TimestampFormatter[] timestampFormatters;

    public EmbulkWriteSupport(Schema schema, TimestampFormatter[] timestampFormatters) {
        this.schema = schema;
        this.timestampFormatters = timestampFormatters;
    }

    @Override
    public WriteContext init(Configuration configuration) {
        if (writeContext == null) {
            init();
        }
        return writeContext;
    }

    @Override
    public void prepareForWrite(RecordConsumer recordConsumer) {
        this.consumer = recordConsumer;
    }

    @Override
    public void write(PageReader record) {
        final ColumnVisitor visitor = new ParquetColumnVisitor(record, consumer);
        consumer.startMessage();
        for (Column c : schema.getColumns()) {
            if (!record.isNull(c)) {
                consumer.startField(c.getName(), c.getIndex());
                c.visit(visitor);
                consumer.endField(c.getName(), c.getIndex());
            }
        }
        consumer.endMessage();
    }

    private void init() {
        MessageType messageType = convertSchema(schema);
        Map<String, String> metadata = new HashMap<>();
        writeContext = new WriteContext(messageType, metadata);
    }

    private MessageType convertSchema(Schema schema) {
        SchemaConvertColumnVisitor visitor = new SchemaConvertColumnVisitor();
        schema.visitColumns(visitor);
        String messageName = "embulk";
        return new MessageType(messageName, visitor.getConvertedFields());
    }

    class ParquetColumnVisitor implements ColumnVisitor {
        final PageReader record;
        final RecordConsumer consumer;

        public ParquetColumnVisitor(PageReader record, RecordConsumer consumer) {
            this.record = record;
            this.consumer = consumer;
        }

        @Override
        public void booleanColumn(Column column) {
            if (!record.isNull(column)) {
                consumer.addBoolean(record.getBoolean(column));
            }
        }

        @Override
        public void longColumn(Column column) {
            if (!record.isNull(column)) {
                consumer.addLong(record.getLong(column));
            }
        }

        @Override
        public void doubleColumn(Column column) {
            if (!record.isNull(column)) {
                consumer.addDouble(record.getDouble(column));
            }
        }

        @Override
        public void stringColumn(Column column) {
            if (!record.isNull(column)) {
                consumer.addBinary(Binary.fromString(record.getString(column)));
            }
        }

        @Override
        public void timestampColumn(Column column) {
            if (!record.isNull(column)) {
                Timestamp t = record.getTimestamp(column);
                String formatted = timestampFormatters[column.getIndex()].format(t);
                consumer.addBinary(Binary.fromString(formatted));
            }
        }
    }

    class SchemaConvertColumnVisitor implements ColumnVisitor {
        List<Type> fields = new ArrayList<>();

        @Override
        public void booleanColumn(Column column) {
            fields.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveTypeName.BOOLEAN, column.getName()));
        }

        @Override
        public void longColumn(Column column) {
            fields.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveTypeName.INT64, column.getName()));
        }

        @Override
        public void doubleColumn(Column column) {
            fields.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveTypeName.DOUBLE, column.getName()));
        }

        @Override
        public void stringColumn(Column column) {
            fields.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveTypeName.BINARY, column.getName()));
        }

        @Override
        public void timestampColumn(Column column) {
            // formatted as string
            fields.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveTypeName.BINARY, column.getName()));
        }

        public List<Type> getConvertedFields() {
            return fields;
        }
    }
}
