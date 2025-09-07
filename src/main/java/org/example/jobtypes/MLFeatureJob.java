package org.example.jobtypes;

import com.datastax.driver.mapping.Mapper;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.example.Transaction;
import org.example.TransactionCount;
import org.example.TransactionCounter;

public class MLFeatureJob implements JobType{

    @Override
    public void runJob(StreamExecutionEnvironment env, DataStream<Transaction> transactionDataStream) throws Exception {
        long checkpointDuration = 100l;

        DataStream<TransactionCount> processedStream = transactionDataStream
                .keyBy(transaction -> transaction.user_id)
                .map(new TransactionCounter());
        processedStream.print();

        //wriiting to cassandra
        CassandraSink.addSink(processedStream)
                .setHost("127.0.0.1")
                .setMapperOptions(() -> new Mapper.Option[]{Mapper.Option.saveNullFields(true)})
                .build();

        //writing to file -- TODO to use kafka instead of file
        FileSink<TransactionCount> parquetSink = FileSink
                .forBulkFormat(new Path("file:///tmp/flink-parquet-output/ml-feature"),
                        ParquetAvroWriters.forReflectRecord(TransactionCount.class))
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .build();

        processedStream.rescale().sinkTo(parquetSink).setParallelism(1);

        env.enableCheckpointing(checkpointDuration);
    }
}
