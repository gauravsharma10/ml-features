package org.example;

import com.datastax.driver.mapping.Mapper;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.example.jobtypes.JobTypeManager;


import java.io.File;


public class TransactionsMLFeaturesJob {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        String runMode = args[0];
        
        System.out.println("Starting Flink job...");


        Schema schema = new Schema.Parser().parse(new File("src/main/resources/transaction.avsc"));
        KafkaSource<GenericRecord> source = KafkaSource.<GenericRecord>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("transactions")
                .setGroupId("transactions-ml-features-job-user")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(ConfluentRegistryAvroDeserializationSchema.forGeneric(schema,"http://localhost:8081"))
                .build();

        DataStream<GenericRecord> sourceStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        DataStream<Transaction> transactionDataStream = sourceStream.map(Transaction::new);
        JobTypeManager.getInstance().getJobType(runMode).runJob(env, transactionDataStream);
        System.out.println("Submitting job to Flink...");
        env.execute(runMode + " Job");
    }
}
