package org.example;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Table;

@Table(keyspace = "transaction", name = "transactioncount")
public class TransactionCount {

    @Column(name = "user_id")
    public String user_id;

    @Column(name = "timestamp")
    public long timestamp;

    @Column(name = "total_transactions_count")
    public long total_transactions_count;



    public TransactionCount() {}

    public TransactionCount(long timestamp, String userId, long count) {
        setTimestamp(timestamp);
        setUser_id(userId);
        setTotal_transactions_count(count);
    }

    @Override
    public String toString() {
        return "TransactionCount{" +
                "timestamp=" + this.getTimestamp() +
                "user_id=" + this.getUser_id() +
                ", total_transactions_count=" + this.getTotal_transactions_count() +
                '}';
    }

    public long getTotal_transactions_count() {
        return total_transactions_count;
    }

    public void setTotal_transactions_count(long total_transactions_count) {
        this.total_transactions_count = total_transactions_count;
    }

    public String getUser_id() {
        return user_id;
    }

    public void setUser_id(String user_id) {
        this.user_id = user_id;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}
