<?php

namespace Bschmitt\Amqp;

use Closure;
use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Support\Arr;
use PhpAmqpLib\Exception\AMQPTimeoutException;
use PhpAmqpLib\Message\AMQPMessage;

/**
 * @author Björn Schmitt <code@bjoern.io>
 */
class MultiConsumer extends Request
{
    /**
     * @var int
     */
    protected $messageCount = 0;

    public array $multiArray = [];

    /**
     * @param string $queue
     * @param Closure $closure
     * @return bool
     * @throws \Exception
     */
    public function consume(array $params): bool
    {
        foreach ($params as $param) {
            $queue = Arr::get($param, 'queue', '');
            $callback = Arr::get($param, 'callback');

            $consumerTag = sprintf('laravel-microservice.%s.', $queue) . getmypid().'-'.uniqid();
            $this->channel->basic_consume($queue, $consumerTag, false, false, false, false, $callback);
        }

        while ($this->channel->is_consuming()) {
            $this->channel->wait();
        }
        return true;
    }

    public function setup()
    {
        $this->connect();

        foreach ($this->multiArray as $param) {

            $queue = Arr::get($param, 'queue', '');
            $exchange = Arr::get($param, 'exchange', '');

            $this->channel->exchange_declare(
                $exchange,
                Arr::get($param, 'exchange_type', 'direct'),
                Arr::get($param, 'exchange_passive', false),
                Arr::get($param, 'exchange_durable', true),
                Arr::get($param, 'exchange_auto_delete', false),
                Arr::get($param, 'exchange_internal', false),
                Arr::get($param, 'exchange_nowait', false),
                Arr::get($param, 'exchange_properties', [])
            );

            $this->queueInfo = $this->channel->queue_declare(
                $queue,
                $this->getProperty('queue_passive'),
                $this->getProperty('queue_durable'),
                $this->getProperty('queue_exclusive'),
                $this->getProperty('queue_auto_delete'),
                $this->getProperty('queue_nowait'),
                $this->getProperty('queue_properties')
            );
            foreach ((array)Arr::get($param, 'routing') as $routingKey) {
                $this->channel->queue_bind(
                    $queue ?: $this->queueInfo[0],
                    $exchange,
                    $routingKey
                );
            }
            $this->connection->set_close_on_destruct(true);
        }
    }
}
