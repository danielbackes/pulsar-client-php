<?php
/**
 * Created by PhpStorm.
 * User: Sunny
 * Date: 2022/6/26
 * Time: 2:39 PM
 */

namespace Pulsar;


use Pulsar\Exception\IOException;
use Pulsar\Exception\MessageNotFound;
use Pulsar\Exception\OptionsException;
use Pulsar\Exception\RuntimeException;
use Pulsar\Proto\BaseCommand\Type;
use Pulsar\Proto\CommandAckResponse;
use Pulsar\Proto\CommandMessage;
use Pulsar\Util\Buffer;
use Pulsar\Util\Helper;
use Pulsar\Util\Packer;
use SplPriorityQueue;
use SplQueue;
use Throwable;

/**
 * Class Consumer
 *
 * @package Pulsar
 */
class Consumer extends Client
{

    /**
     * @var SplQueue
     */
    protected $messageQueue;


    /**
     * @var ConsumerOptions
     */
    protected $options;


    /**
     * @var \SplPriorityQueue
     */
    protected $nackMessageQueue;


    /**
     * @var array<PartitionConsumer>
     */
    protected $consumers = [];



    /**
     * @param string $url
     * @param ConsumerOptions $options
     * @throws OptionsException
     */
    public function __construct(string $url, ConsumerOptions $options)
    {
        parent::__construct($url, $options);
    }


    /**
     * @return void
     * @throws Exception\IOException
     * @throws OptionsException
     */
    public function connect()
    {
        //
        $this->messageQueue = new SplQueue();
        $this->nackMessageQueue = new SplPriorityQueue();
        $this->nackMessageQueue->setExtractFlags(SplPriorityQueue::EXTR_BOTH);

        parent::initialization();

        // Send Subscribe Command
        foreach ($this->topicManage->all() as $id => $topic) {
            $this->consumers[ $id ] = new PartitionConsumer(
                $id,
                $topic,
                $this->topicManage->getConnection($topic),
                $this->options
            );
        }
    }


    /**
     * @param array $policy
     * @return bool
     * @throws IOException
     */
    protected function reconnect(array $policy): bool
    {
        /**
         * @var $policy array{status: bool,interval: int,limit: int}
         */

        $this->reconnect += 1;
        echo sprintf("consumer reconnect[%d] %s %s\n",
            $this->reconnect,
            $this->url,
            date('Y-m-d H:i:s')
        );

        try {
            // clear status
            $this->connections = [];
            $this->consumers = [];

            // connect
            $this->fetchPartitionTopicMetadata();
            $this->connect();
        } catch (Throwable $e) {

            if ($policy['limit'] >= 1 && $this->reconnect >= $policy['limit']) {
                throw new IOException('Reconnection Fail');
            }

            // Preventing memory overflows
            $interval = $policy['interval'] > 0 ? $policy['interval'] : 5;
            sleep($interval);

            return $this->reconnect($policy);
        }

        echo sprintf("reconnect %s success %s\n", $this->url, date('Y-m-d H:i:s'));
        $this->reconnect = 0;
        return true;
    }

    /**
     * @return void
     * @throws \Exception
     */
    protected function flow()
    {
        foreach ($this->consumers as $consumer) {
            $consumer->flow();
        }
    }


    /**
     * @param bool $loop
     * @return Message
     * @throws IOException
     * @throws RuntimeException
     * @throws MessageNotFound
     */
    public function receive(bool $loop = true): Message
    {
        if (!$this->isHandshake) {
            throw new RuntimeException('not connect to pulsar server');
        }

        // send FLOW command
        $this->flow();

        // get message from local queue
        if (!$this->messageQueue->isEmpty()) {
            return $this->messageQueue->dequeue();
        }

        $response = $this->pollFrame($this->getWaitSeconds());

        // nack
        $this->executeInternalNack();
    
        if (is_null($response)) {
            if (!$loop) {
                throw new MessageNotFound();
            }
            return $this->receive($loop);
        }


        /**
         * @var $commandMessage CommandMessage
         */
        $commandMessage = $response->getSubCommand();
        // It may appear that the current message is not CommandMessage
        if (!( $commandMessage instanceof CommandMessage )) {
            if (!$loop) {
                throw new MessageNotFound('command parse fail', MessageNotFound::CommandParseFail);
            }
            return $this->receive($loop);
        }

        $this->enqueueCommandMessage($commandMessage, $response->getBuffer());

        return $this->messageQueue->dequeue();
    }

    /**
     * @return array<Message>
     * @throws IOException
     * @throws MessageNotFound
     * @throws RuntimeException
     */
    public function batchReceive(bool $loop = true): array
    {
        $messages = [$this->receive($loop)];
        while (!$this->messageQueue->isEmpty()) {
            $messages[] = $this->messageQueue->dequeue();
        }

        return $messages;
    }

    /**
     * Sends the CommandAck and blocks for its CommandAckResponse, correlated by request ID.
     *
     * While waiting, any MESSAGE frame that arrives first (the broker may deliver one before
     * the ACK_RESPONSE, since the connection is asynchronous) is queued rather than discarded.
     * If the connection is already dead -- or drops while waiting -- sendAckRequest() reconnects
     * and resends the CommandAck with a fresh request ID on the new connection before (re)waiting.
     *
     * @param Message $message
     * @return CommandAckResponse|null
     * @throws IOException
     * @throws RuntimeException
     */
    public function ack(Message $message): ?CommandAckResponse
    {
        if (!$message->canAck()) {
            return null;
        }

        $requestId = $this->sendAckRequest($message);

        $deadline = microtime(true) + $this->options->getAckTimeout();

        do {
            $remaining = $deadline - microtime(true);
            if ($remaining <= 0) {
                throw new RuntimeException('Timed out waiting for ACK response.');
            }

            // Select::wait() forwards this to stream_select()'s tv_sec, which requires an int
            $response = $this->pollFrame((int) ceil($remaining), function () use ($message, &$requestId) {
                // the connection was rebuilt; the outstanding ACK request died with it
                $requestId = $this->sendAckRequest($message);
            });

            if (null === $response) {
                continue;
            }

            $baseCommand = $response->getBaseCommand();

            $commandType = $baseCommand->getType();

            if (Type::CLOSE_CONSUMER_VALUE === $commandType->value()) {
                // only abort if it's the consumer this message belongs to; the connection
                // may be shared with other partition consumers that closed independently
                if ($baseCommand->getCloseConsumer()->getConsumerId() === $message->getConsumerID()) {
                    throw new RuntimeException(
                        'The consumer was closed before the message acknowledgment was confirmed.'
                    );
                }
                continue;
            }

            if (Type::MESSAGE_VALUE === $commandType->value()) {
                $this->enqueueCommandMessage($baseCommand->getMessage(), $response->getBuffer());
                continue;
            }

            if (Type::ACK_RESPONSE_VALUE === $commandType->value()) {
                $ackResponse = $baseCommand->getAckResponse();

                if ($ackResponse->getRequestId() !== $requestId) {
                    throw new RuntimeException('ACK response request ID does not match.');
                }

                if ($ackResponse->hasError()) {
                    $msg = $ackResponse->hasMessage() ? $ackResponse->getMessage() : $ackResponse->getError()->name();
                    throw new RuntimeException(
                        sprintf('The broker rejected the acknowledgment: %s', $msg),
                        $ackResponse->getError()->value()
                    );
                }

                return $ackResponse;
            }

        } while (true);
    }


    /**
     * @param Message $message
     * @return void
     * @throws \Exception
     */
    public function nack(Message $message)
    {
        if (!$message->canAck()) {
            return;
        }

        // Is it necessary to enter the dead letter queue
        $deadLetter = $this->options->getDeadLetterPolicy();
        if ($deadLetter->trigger($message)) {
            $this->ack($message);
            return;
        }

        // push to Local queue
        $this->nackMessageQueue->insert($message, -( time() + $this->options->getNackRedeliveryDelay() ));
    }


    /**
     * @return void
     * @throws \Exception
     */
    protected function executeInternalNack()
    {
        if ($this->nackMessageQueue->isEmpty()) {
            return;
        }

        for ($i = 0; $i < $this->nackMessageQueue->count(); $i++) {
            $priority = $this->nackMessageQueue->top()['priority'];

            if (time() < -$priority) {
                return;
            }

            /**
             * @var $message Message
             */
            $message = $this->nackMessageQueue->extract()['data'];

            // send CommandRedeliverUnacknowledgedMessages
            $this->getPartitionConsumer($message->getConsumerID())->nack($message);
        }
    }



    /**
     * @return void
     * @throws Exception\IOException
     */
    public function close()
    {
        // Send Close Command
        foreach ($this->consumers as $consumer) {
            $consumer->close();
        }

        // Close tcp connection
        parent::close();
    }


    /**
     * @return int|mixed
     */
    protected function getWaitSeconds()
    {
        if ($this->nackMessageQueue->isEmpty()) {
            return $this->options->getNackRedeliveryDelay();
        }

        /**
         * @var $message Message
         */
        $priority = $this->nackMessageQueue->top()['priority'];

        return max(-$priority - time(), 0);
    }


    /**
     * @param int $consumerID
     * @return PartitionConsumer
     */
    protected function getPartitionConsumer(int $consumerID): PartitionConsumer
    {
        return $this->consumers[ $consumerID ];
    }

    /**
     * Waits for a single frame from the event loop, applying the reconnect policy on
     * connection loss and running the ping bookkeeping.
     *
     * On a dropped connection, once it's rebuilt, $onReconnect (if given) is invoked to
     * resend whatever the caller had outstanding on the old connection -- a CommandAck,
     * for instance, has no way to be resumed on a new one. Either way this returns null,
     * same as a plain timeout, so the caller's own retry loop drives the next poll.
     *
     * @param int|float|null $timeoutSeconds
     * @param callable|null $onReconnect called with no arguments right after a successful reconnect
     * @return Response|null
     * @throws IOException
     */
    private function pollFrame($timeoutSeconds = null, ?callable $onReconnect = null): ?Response
    {
        try {
            $response = $this->eventloop->wait($timeoutSeconds);

            $this->ping();

            return $response;
        } catch (IOException $e) {
            $policy = $this->options->getReconnectPolicy();

            // not enable reconnect
            if (!$policy['status']) {
                throw $e;
            }

            $this->reconnect($policy);

            if ($onReconnect) {
                $onReconnect();
            }

            return null;
        }
    }

    /**
     * Sends a CommandAck for $message with a fresh request ID, applying the reconnect policy
     * (and retrying the send on the rebuilt connection) if the connection is already dead.
     *
     * @param Message $message
     * @return int the request ID the ack was sent with
     * @throws IOException
     */
    private function sendAckRequest(Message $message): int
    {
        $requestId = Helper::getRequestID();

        try {
            $this->getPartitionConsumer($message->getConsumerID())->ack($message, $requestId);
        } catch (IOException $e) {
            $policy = $this->options->getReconnectPolicy();

            // not enable reconnect
            if (!$policy['status']) {
                throw $e;
            }

            $this->reconnect($policy);

            return $this->sendAckRequest($message);
        }

        return $requestId;
    }

    /**
     * Decodes a MESSAGE frame's payload into Message objects, queues them locally, and
     * decrements the partition's available flow-control permits accordingly.
     *
     * Shared by receive() and ack()'s wait loop, since a MESSAGE frame can arrive while
     * ack() is waiting on the same connection for an unrelated ACK_RESPONSE -- it must be
     * queued here rather than discarded, or the message would be silently lost.
     *
     * @param CommandMessage $commandMessage
     * @param Buffer $buffer
     * @return void
     */
    private function enqueueCommandMessage(CommandMessage $commandMessage, Buffer $buffer)
    {
        $consumer = $this->getPartitionConsumer($commandMessage->getConsumerId());

        /**
         * @var array<Message> $messages
         */
        $messages = Packer::decode($commandMessage, $buffer, $consumer->getTopic());

        foreach ($messages as $message) {
            // Save Options to Message Object
            $message->setOptions($this->options);

            $this->messageQueue->enqueue($message);
        }

        $consumer->decrement(sizeof($messages));
    }

}
