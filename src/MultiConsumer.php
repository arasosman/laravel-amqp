<?php

namespace Bschmitt\Amqp;

use Closure;
use Exception;
use Illuminate\Support\Arr;

/**
 * @author Björn Schmitt <code@bjoern.io>
 */
class MultiConsumer extends Request
{
    public array $multiArray = [];
    /**
     * @var int
     */
    protected int $messageCount = 0;

    /**
     * @param array $params
     * @return bool
     */
    public function consume(array $params): bool
    {
        foreach ($params as $param) {
            $queue = Arr::get($param, 'queue', '');
            $callback = Arr::get($param, 'callback');

            $consumerTag = sprintf('laravel-microservice.%s.', $queue) . getmypid() . '-' . uniqid();
            $this->channel->basic_consume($queue, $consumerTag, false, false, false, false, $callback);
        }

        while ($this->channel->is_consuming()) {
            $this->channel->wait();
        }
        return true;
    }

    public function setup(): void
    {
        $this->connect();

        foreach ($this->multiArray as $param) {

            $queue = Arr::get($param, 'queue', '');
            $exchange = Arr::get($param, 'exchange', '');

            $this->channel->exchange_declare(
                $exchange,
                Arr::get($param, 'exchange_type', 'fanout'),
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
