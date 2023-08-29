using Confluent.Kafka;
using System.Text;
using static Confluent.Kafka.ConfigPropertyNames;
using System.Threading;

string kafkaEndpoint = "127.0.0.1:9092";

// The Kafka topic we'll be using
string kafkaTopic = "testtopic";

var producerConfig = new ProducerConfig { BootstrapServers = kafkaEndpoint };

using (var producer = new ProducerBuilder<Null, string>(producerConfig).Build())
{
    // Send 10 messages to the topic
    for (int i = 0; i < 10; i++)
    {
        var message = $"Event {i}";
        var result = producer.ProduceAsync(kafkaTopic, new Message<Null, string> { Value = message }).GetAwaiter().GetResult();
        Console.WriteLine($"Event {i} sent on Partition: {result.Partition} with Offset: {result.Offset}");
    }
}


var consumerConfig = new ConsumerConfig
{
    GroupId = "myconsumer",
    BootstrapServers = kafkaEndpoint,
    AutoOffsetReset = AutoOffsetReset.Earliest
};
using (var consumerBuilder = new ConsumerBuilder<Ignore, string>(consumerConfig).Build())
{
    consumerBuilder.Subscribe(kafkaTopic);

    while (true)
    {
        var consumeResult = consumerBuilder.Consume();

        Console.WriteLine(consumeResult.Message.Value);
    }

    consumerBuilder.Close();
}