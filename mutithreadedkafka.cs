using System;
using System.Collections.Generic;
using System.Threading;
using Confluent.Kafka;

public class ConsumerWorker
{
    private readonly ConsumerConfig _config;
    private readonly string _topic;
    private readonly int _partition;
    private readonly CancellationTokenSource _cts;

    public ConsumerWorker(ConsumerConfig config, string topic, int partition, CancellationTokenSource cts)
    {
        _config = config;
        _topic = topic;
        _partition = partition;
        _cts = cts;
    }

    public void Run()
    {
        using var consumer = new ConsumerBuilder<string, string>(_config).Build();
        consumer.Assign(new TopicPartitionOffset(_topic, new Partition(_partition), Offset.Beginning));

        try
        {
            while (!_cts.Token.IsCancellationRequested)
            {
                var consumeResult = consumer.Consume(_cts.Token);
                // Process the consumeResult here
                // Commit offsets manually
            }
        }
        catch (OperationCanceledException)
        {
            // Handle cancellation
        }
        finally
        {
            consumer.Close();
        }
    }
}

public class MultiThreadedConsumer
{
    private readonly ConsumerConfig _config;
    private readonly string _topic;
    private readonly CancellationTokenSource _cts;
    private readonly int _numWorkers;

    public MultiThreadedConsumer(ConsumerConfig config, string topic, int numWorkers)
    {
        _config = config;
        _topic = topic;
        _numWorkers = numWorkers;
        _cts = new CancellationTokenSource();
    }

    public void Run()
    {
        var topicMetadata = _config.BootstrapServers.GetMetadata(TimeSpan.FromSeconds(10), _topic);
        var partitions = topicMetadata.Topics[0].Partitions;

        var workers = new List<Thread>();

        for (int i = 0; i < Math.Min(_numWorkers, partitions.Count); i++)
        {
            var worker = new ConsumerWorker(_config, _topic, partitions[i], _cts);
            var thread = new Thread(worker.Run);
            workers.Add(thread);
            thread.Start();
        }

        // Wait for all workers to finish
        foreach (var worker in workers)
        {
            worker.Join();
        }
    }
}
