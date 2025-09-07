package org.example.jobtypes;

import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.example.Transaction;

public class BackUpJob implements JobType {

    @Override
    public void runJob(StreamExecutionEnvironment env, DataStream<Transaction> transactionDataStream) {
        long checkpointDuration  = 6000l;
        FileSink<Transaction> parquetSink = FileSink
                .forBulkFormat(new Path("file:///tmp/flink-parquet-output/backup"),
                        ParquetAvroWriters.forReflectRecord(Transaction.class))
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .build();
        transactionDataStream.rescale().sinkTo(parquetSink).setParallelism(1);
        env.enableCheckpointing(checkpointDuration);
    }
}
