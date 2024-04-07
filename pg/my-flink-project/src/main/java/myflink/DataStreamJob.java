/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package myflink;
import com.ververica.cdc.connectors.sqlserver.SqlServerSource;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.nio.charset.StandardCharsets;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;

class CustomSerializationSchema implements KafkaRecordSerializationSchema<String> {

	private String topic;

	public CustomSerializationSchema() {}

	public CustomSerializationSchema(String topic) {
		this.topic = topic;
	}

	@Override
	public ProducerRecord<byte[], byte[]> serialize(String element, KafkaSinkContext context, Long timestamp) {

		System.out.println("msg: " + element.toString());

		return new ProducerRecord<byte[], byte[]>(topic, element.toString().getBytes(StandardCharsets.UTF_8));
	}
}

public class DataStreamJob {

	public static void main(String[] args) throws Exception {
		ParameterTool parameters = ParameterTool.fromArgs(args);

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		SourceFunction<String> sourceFunction = SqlServerSource.<String>builder()
				.hostname(parameters.getRequired("ARG:SQLSERVER_HOST"))  // ex: 127.0.0.1
				.port(Integer.parseInt(parameters.get("ARG:SQLSERVER_PORT", "1433")))
				.database(parameters.getRequired("ARG:SQLSERVER_DATABASE"))
				.tableList(parameters.get("ARG:SQLSERVER_TABLES", "dbo.*")) // ex: dbo.rockets, dbo.*
				.username(parameters.getRequired("ARG:SQLSERVER_USERNAME"))
				.password(parameters.getRequired("ARG:SQLSERVER_PASSWORD"))
				.deserializer(new JsonDebeziumDeserializationSchema())
				.build();

		DataStream<String> stream = env.addSource(sourceFunction);

//		stream.print().setParallelism(1);

		KafkaSink<String> sink = KafkaSink.<String>builder()
				.setBootstrapServers(parameters.getRequired("ARG:KAFKA_BROKERS"))
				.setProperty("sasl.mechanism", "PLAIN")
				.setRecordSerializer(new CustomSerializationSchema(parameters.getRequired("ARG:KAFKA_TOPIC")))
				.setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
				.build();

		stream.sinkTo(sink);

		env.execute("Flink Java API Skeleton");
	}
}
