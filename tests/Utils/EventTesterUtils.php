<?php

namespace Keboola\Test\Utils;

use Keboola\StorageApi\Client;
use Keboola\StorageApi\Event;
use Retry\BackOff\FixedBackOffPolicy;
use Retry\BackOff\LinearBackOffPolicy;
use Retry\Policy\SimpleRetryPolicy;
use Retry\RetryProxy;

/**
 * Use with \Keboola\Test\StorageApiTestCase class.
 *
 * - in `setUp` initialize `initTest()`
 * - in test use `listEvent()` and `assertEvent()`
 */
trait EventTesterUtils
{
    /**
     * @var string
     */
    protected $tokenId;

    /**
     * @var string
     */
    protected $lastEventId;

    /**
     * @return array
     * @throws \Exception
     */
    public function createAndWaitForEvent(Event $event, ?Client $sapiClient = null)
    {
        // @phpstan-ignore property.notFound
        $client = null !== $sapiClient ? $sapiClient : $this->_client;

        $id = $client->createEvent($event);

        /** @var array $return */
        $return = $this->retryWithCallback(
            fn() => $client->getEvent($id),
        );
        return $return;
    }

    /**
     * @uses \Keboola\Test\StorageApiTestCase::createAndWaitForEvent()
     * @return array<mixed>
     */
    protected function initEvents(Client $client): array
    {
        // use default _client; branch client doesn't support verifyToken call
        $this->tokenId = $client->verifyToken()['id'];

        $fireEvent = (new Event())
            ->setComponent('dummy')
            ->setMessage('dummy');
        $lastEvent = $this->createAndWaitForEvent($fireEvent, $client);

        if (!empty($lastEvent)) {
            $this->lastEventId = $lastEvent['id'];
        }

        return $lastEvent;
    }

    public function assertEventsCallback(Client $client, callable $callback, int $limit = 1): array
    {
        return (array) $this->retryWithCallback(function () use ($client, $limit) {
            return $client->listEvents([
                'sinceId' => $this->lastEventId,
                'limit' => $limit,
                'q' => sprintf('token.id:%s', $this->tokenId),
            ]);
        }, $callback);
    }

    /**
     * @return mixed
     */
    protected function retryWithCallback(callable $apiCall, ?callable $callback = null)
    {
        sleep(2); // wait for ES to refresh
        $retryPolicy = new SimpleRetryPolicy(30);
        $proxy = new RetryProxy($retryPolicy, new LinearBackOffPolicy(
            250,
            250,
        ));
        return $proxy->call(function () use ($apiCall, $callback) {
            $events = $apiCall();
            if ($callback !== null) {
                $callback($events);
            }
            return $events;
        });
    }

    public function assertEventWithRetries(
        Client $client,
        callable $assertCallback,
        EventsQueryBuilder $query,
        int $limit = 20
    ): void {
        $query = $query->generateQuery();

        $apiCall = fn() => $client->listEvents([
                'sinceId' => $this->lastEventId,
                'limit' => $limit,
                'q' => $query,
            ]);

        $this->retryWithCallback($apiCall, $assertCallback);
    }

    /**
     * @param int|string|null $expectedObjectId
     */
    protected function listEvents(Client $client, string $eventName, $expectedObjectId = null, int $limit = 1): array
    {
        return $this->retry(function () use ($client, $expectedObjectId, $limit) {
            $tokenEvents = $client->listEvents([
                'sinceId' => $this->lastEventId,
                'limit' => $limit,
                'q' => sprintf('token.id:%s', $this->tokenId),
            ]);

            if ($expectedObjectId === null) {
                return $tokenEvents;
            }

            return array_filter($tokenEvents, function ($event) use ($expectedObjectId) {
                return $event['objectId'] === $expectedObjectId;
            });
        }, 20, $eventName);
    }

    /**
     * @param callable $apiCall
     * @param int $retries
     * @param string $eventName
     * @return array
     */
    private function retry($apiCall, $retries, $eventName)
    {
        $retryPolicy = new SimpleRetryPolicy($retries);
        $proxy = new RetryProxy($retryPolicy, new LinearBackOffPolicy(
            250,
            250,
        ));
        /** @var array $proxiedCallResult */
        $proxiedCallResult = $proxy->call(function () use ($apiCall, $eventName) {
            /** @var array $events */
            $events = $apiCall();

            $this->assertNotEmpty($events, 'There were no events');

            $eventsNames = array_column($events, 'event');
            $this->assertContainsEquals($eventName, $eventsNames, sprintf('Event does not matches "%s"', $eventName));

            return $events;
        });
        return $proxiedCallResult;
    }

    /**
     * @param array $event
     * @param string $expectedEventName
     * @param string $expectedEventMessage
     * @param mixed $expectedObjectId
     * @param string $expectedObjectName
     * @param string $expectedObjectType
     * @param array $expectedParams
     * @return void
     */
    protected function assertEvent(
        $event,
        $expectedEventName,
        $expectedEventMessage,
        $expectedObjectId,
        $expectedObjectName,
        $expectedObjectType,
        $expectedParams
    ) {
        self::assertArrayHasKey('objectName', $event);
        self::assertEquals($expectedObjectName, $event['objectName']);
        self::assertArrayHasKey('objectType', $event);
        self::assertEquals($expectedObjectType, $event['objectType']);
        self::assertArrayHasKey('objectId', $event);
        self::assertEquals($expectedObjectId, $event['objectId']);
        self::assertArrayHasKey('event', $event);
        self::assertEquals($expectedEventName, $event['event']);
        self::assertArrayHasKey('message', $event);
        self::assertEquals($expectedEventMessage, $event['message']);
        self::assertArrayHasKey('token', $event);
        self::assertEquals($this->tokenId, $event['token']['id']);
        self::assertArrayHasKey('params', $event);
        self::assertSame($expectedParams, $event['params']);
        $this->assertEventUuid($event);
    }

    protected function assertEventUuid(array $event): void
    {
        $this->assertArrayHasKey('uuid', $event);
        $this->assertMatchesRegularExpression('/^[0-9a-f]{8}(?:\-[0-9a-f]{4}){3}-[0-9a-f]{12}$/', $event['uuid']);
    }
}
