package org.example.jobtypes;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.Transaction;

public interface JobType {

    public void runJob(StreamExecutionEnvironment env, DataStream<Transaction> transactionDataStream) throws Exception;
}
