package org.example;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;

public class TransactionCounter extends RichMapFunction<Transaction, TransactionCount> {

    private transient ValueState<Long> countState;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Long> descriptor =
                new ValueStateDescriptor<>(
                        "transactionCount",
                        Long.class);

        countState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public TransactionCount map(Transaction transaction) throws Exception {

        Long currentCount = countState.value();
        if (currentCount == null) {
            currentCount = 0L;
        }

        currentCount++;
        countState.update(currentCount);

        return new TransactionCount(System.currentTimeMillis(), transaction.user_id, currentCount);
    }
}