<?php

namespace Keboola\Test\Utils;

use Keboola\StorageApi\Client;
use Keboola\StorageApi\Event;
use Retry\BackOff\FixedBackOffPolicy;
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
     * @uses \Keboola\Test\StorageApiTestCase::createAndWaitForEvent()
     * @return void
     */
    protected function initEvents(Client $client)
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
    }

    /**
     * @param string $eventName
     * @return array
     */
    protected function listEvents(Client $client, $eventName, $expectedObjectId = null)
    {
        return $this->retry(function () use ($client, $expectedObjectId) {
            $tokenEvents = $client->listEvents([
                'sinceId' => $this->lastEventId,
                'limit' => 1,
                'q' => sprintf('token.id:%s', $this->tokenId),
            ]);

            if ($expectedObjectId === null) {
                return $tokenEvents;
            }

            return array_filter($tokenEvents, function ($event) use ($expectedObjectId) {
                return $event['objectId'] === $expectedObjectId;
            });
        }, 10, $eventName);
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
        $proxy = new RetryProxy($retryPolicy, new FixedBackOffPolicy(250));
        /** @var array $proxiedCallResult */
        $proxiedCallResult = $proxy->call(function () use ($apiCall, $eventName) {
            /** @var array $events */
            $events = $apiCall();

            $this->assertNotEmpty($events, 'There were no events');
            $this->assertEquals($eventName, $events[0]['event'], sprintf('Event does not matches "%s"', $eventName));

            return $events;
        });
        return $proxiedCallResult;
    }

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
    }
}
