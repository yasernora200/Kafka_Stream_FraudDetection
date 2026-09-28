package com.nora.streams;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nora.events.Transaction;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@Configuration
@EnableKafkaStreams
@Slf4j
public class FraudDetectionStream {

    //create bean
    //-> read the topic
    //-> process filter
    //-> write to destination

    @Bean
    public KStream<String, String> fraudDetectStream(StreamsBuilder builder) {

        // Step 1: Read messages from the input topic.
        KStream<String, String> transactionsStream = builder
                .stream("transactions");

        // Step 2: Process the stream to detect fraudulent transactions.

        KStream<String, String> fraudTransactionStream = transactionsStream
                .filter((key, value) -> isSuspicious(value))
                .peek((key, value) -> {
                    log.warn("⚠️ FRAUD ALERT - transactionId={} , value={}", key, value);
                });

        // Step 3: write detected fraudulent transactions to an output topic.
        fraudTransactionStream.to("fraud-alerts");

        return transactionsStream;

    }



    private boolean isSuspicious(String value) {
        try {
            Transaction transaction = new ObjectMapper()
                    .readValue(value, Transaction.class); // validate JSON
            return transaction.amount() > 10000; // simple fraud rule
        } catch (Exception e) {
            return false;
        }
    }
}
