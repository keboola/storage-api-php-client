<?php
/**
 *
 * User: Martin Halamíček
 * Date: 16.5.12
 * Time: 11:46
 *
 */

namespace Keboola\Test\Common;

use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Event;
use Keboola\Test\StorageApiTestCase;
use Keboola\Test\Utils\EventsQueryBuilder;

class EventsTest extends StorageApiTestCase
{
    public function testEventCreate(): void
    {
        $event = new Event();
        $event->setComponent('ex-sfdc')
            ->setConfigurationId('sys.c-sfdc.account-01')
            ->setDuration(200)
            ->setType('info')
            ->setRunId('ddddssss')
            ->setMessage('Table Opportunity fetched.')
            ->setDescription('Some longer description of event')
            ->setParams([
                'accountName' => 'Keboola',
                'configuration' => 'sys.c-sfdc.sfdc-01',
            ]);

        $savedEvent = $this->createAndWaitForEvent($event);

        $assert = function (Event $expected, array $event) {
            $this->assertEquals($expected->getComponent(), $event['component']);
            $this->assertEquals($expected->getConfigurationId(), $event['configurationId']);
            $this->assertEquals($expected->getDuration(), $event['performance']['duration']);
            $this->assertEquals($expected->getType(), $event['type']);
            $this->assertEquals($expected->getRunId(), $event['runId']);
            $this->assertEquals($expected->getMessage(), $event['message']);
            $this->assertEquals($expected->getDescription(), $event['description']);
            $this->assertEquals($expected->getParams(), $event['params']);
            $this->assertGreaterThan(0, $event['idBranch']);
            $this->assertEventUuid($event);
        };
        $assert($event, $savedEvent);

        $savedEvent = $this->_client->getEvent($savedEvent['uuid']);
        $assert($event, $savedEvent);
    }

    public function testEventWithSchemaMatchesTheSchema(): void
    {
        $event = new Event();
        $event->setComponent('keboola.keboola-as-code')
            ->setMessage('Sync-pull command done.')
            ->setParams(['command' => 'sync-pull'])
            ->setResults(['projectId' => 13]);

        $savedEvent = $this->createAndWaitForEvent($event);
        $this->assertSame('ext.keboola.keboola-as-code.', $savedEvent['event']);
    }

    public function testEventWithSchemaDoesNotMatchTheSchema(): void
    {
        $event = new Event();
        $event->setComponent('keboola.keboola-as-code')
            ->setMessage('Sync-pull command done.')
            ->setParams(['command' => 'sync-pull']);
        // projectId is missing from results

        try {
            $this->createAndWaitForEvent($event);
            $this->fail('Should have thrown');
        } catch (ClientException $e) {
            $this->assertSame('storage.event.schemaValidationFailed', $e->getStringCode());
            $this->assertSame(
                [
                    [
                        'key' => 'results.projectId',
                        'message' => 'The property projectId is required',
                    ],
                ],
                $e->getContextParams()['errors'],
            );
        }
    }

    public function testEventsSinceIdMaxId(): void
    {
        $runId = sha1($this->generateDescriptionForTestObject());
        // create 3 events wait for persistence
        $event1= $this->createAndWaitForEvent((new Event())
            ->setComponent('dummy')
            ->setRunId($runId)
            ->setMessage('dummy1'));
        $event2 = $this->createAndWaitForEvent((new Event())
            ->setComponent('dummy')
            ->setRunId($runId)
            ->setMessage('dummy2'));
        $event3 = $this->createAndWaitForEvent((new Event())
            ->setComponent('dummy')
            ->setRunId($runId)
            ->setMessage('dummy3'));

        // list events since first one
        // by id
        $events = $this->_client->listEvents([
            'runId' => $runId,
            'sinceId' => $event1['id'],
        ]);
        $this->assertCount(2, $events);
        // by uuid
        $events = $this->_client->listEvents([
            'runId' => $runId,
            'sinceId' => $event1['uuid'],
        ]);
        $this->assertCount(2, $events);

        // list events between first and last
        // by id
        $events = $this->_client->listEvents([
            'runId' => $runId,
            'sinceId' => $event1['id'],
            'maxId' => $event3['id'],
        ]);
        $this->assertCount(1, $events);
        $this->assertSame($event2['id'], $events[0]['id']);
        $this->assertSame($event2['uuid'], $events[0]['uuid']);
        // by uuid
        $events = $this->_client->listEvents([
            'runId' => $runId,
            'sinceId' => $event1['uuid'],
            'maxId' => $event3['uuid'],
        ]);
        $this->assertCount(1, $events);
        $this->assertSame($event2['id'], $events[0]['id']);
        $this->assertSame($event2['uuid'], $events[0]['uuid']);
        // now do the same but force uuid sorting
        // by uuid
        $events = $this->_client->listEvents([
            'runId' => $runId,
            'sinceId' => $event1['uuid'],
            'maxId' => $event3['uuid'],
            'forceUuid' => true,
        ]);
        $this->assertCount(1, $events);
        $this->assertSame($event2['id'], $events[0]['id']);
        $this->assertSame($event2['uuid'], $events[0]['uuid']);
    }

    public function testEventWithoutSchemaHasOnlyMinimalValidation(): void
    {
        $event = new Event();
        $event->setComponent('whatever')
            ->setMessage('Whatever');
        $savedEvent = $this->createAndWaitForEvent($event);

        $this->assertSame('ext.whatever.', $savedEvent['event']);
    }

    public function testEventCreateWithoutParams(): void
    {
        $event = new Event();
        $event->setComponent('ex-sfdc')
            ->setType('info')
            ->setMessage('Table Opportunity fetched.');

        $event = $this->createAndWaitForEvent($event);

        // to check if params is object we have to convert received json to objects instead of assoc array
        // so we have to use raw Http Client
        $client = $this->getGuzzleClientForClient($this->_client);

        $response = $client->get('/v2/storage/events/' . $event['id']);

        $response = json_decode((string) $response->getBody());

        $this->assertInstanceOf('stdclass', $response->params);
        $this->assertInstanceOf('stdclass', $response->results);
        $this->assertInstanceOf('stdclass', $response->performance);
    }

    /**
     * @dataProvider largeEventWithinMaxSizeLimitDataProvider
     * @param $messageLength
     * @throws \Exception
     */
    public function testLargeEventWithinMaxSizeLimit($messageLength): void
    {
        $largeMessage = str_repeat('x', $messageLength);
        $event = new Event();
        $event->setComponent('ex-sfdc')
            ->setMessage($largeMessage);

        $savedEvent = $this->createAndWaitForEvent($event);
        $this->assertEquals($largeMessage, $savedEvent['message']);
    }

    public function largeEventWithinMaxSizeLimitDataProvider()
    {
        return [
            [10000],
            [50000],
            [64000],
            [128000],
            [190000],
        ];
    }

    public function testLargeEventOverLimitShouldNotBeCreated(): void
    {
        $largeMessage = str_repeat('x', 250000);
        $event = new Event();
        $event->setComponent('ex-sfdc')
            ->setMessage($largeMessage);

        try {
            $this->createAndWaitForEvent($event);
            $this->fail('event should not be created');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('requestTooLarge', $e->getStringCode());
        }
    }

    public function testInvalidType(): void
    {
        $this->expectException(\Keboola\StorageApi\Exception::class);
        $event = new Event();
        $event->setType('homeless');
    }

    public function testEventCompatibility(): void
    {
        $event = new Event();
        $event->setComponentName('sys.c-sfdc.account-01')
            ->setComponentType('ex-sfdc')
            ->setMessage('test');

        $savedEvent = $this->createAndWaitForEvent($event);
        $this->assertEquals($event->getComponentName(), $savedEvent['configurationId']);
        $this->assertEquals($event->getComponentType(), $savedEvent['component']);
    }

    /**
     * http://www.cl.cam.ac.uk/~mgk25/ucs/examples/UTF-8-test.txt
     */
    public function testCreateInvalidUTF8WithFormData(): void
    {
        $message = 'SQLSTATE[XX000]: ' . chr(0x00000080);
        $event = new Event();
        $event->setComponent('ex-sfdc')
            ->setType('info')
            ->setMessage($message);

        try {
            $this->createWithFormDataAndWaitForEvent($event);
            $this->fail('event should not be created');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('malformedRequest', $e->getStringCode());
        }
    }

    /**
     * http://www.cl.cam.ac.uk/~mgk25/ucs/examples/UTF-8-test.txt
     */
    public function testCreateInvalidUTF8(): void
    {
        $message = 'SQLSTATE[XX000]: ' . chr(0x00000080);
        $event = new Event();
        $event->setComponent('ex-sfdc')
            ->setType('info')
            ->setMessage($message);

        $this->expectException(\GuzzleHttp\Exception\InvalidArgumentException::class);
        $this->expectExceptionMessage('json_encode error: Malformed UTF-8 characters, possibly incorrectly encoded');
        $this->createAndWaitForEvent($event);
    }

    public function testEventList(): void
    {
        $this->initEvents($this->_client);
        // at least one event should be generated
        $this->_client->listBuckets();

        $assertCallback = function ($events) {
            $this->assertCount(1, $events);
        };
        $query = new EventsQueryBuilder();
        $this->assertEventWithRetries($this->_client, $assertCallback, $query);
    }

    public function testEventsFiltering(): void
    {
        $this->initEvents($this->_client);

        // we have assign runId to isolate testing events,
        // because if someone displays navigation in KBC "bucketListed" event is created
        $runId = $this->_client->generateId();
        $event = new Event();
        $event
            ->setComponent('transformation')
            ->setRunId($runId)
            ->setType('info')
            ->setMessage('test')
            ->setConfigurationId('myConfig');
        $this->createAndWaitForEvent($event);

        $event->setComponent('ex-fb');
        $this->createAndWaitForEvent($event);
        $event->setMessage('another');
        $this->createAndWaitForEvent($event);

        $assertCallback = function ($events) {
            $this->assertCount(3, $events);
        };
        $query = new EventsQueryBuilder();
        $query->setRunId($runId);
        $this->assertEventWithRetries($this->_client, $assertCallback, $query);

        $assertCallback = function ($events) {
            $this->assertCount(1, $events, 'filter by component');
        };
        $query = new EventsQueryBuilder();
        $query->setComponent('transformation');
        $this->assertEventWithRetries($this->_client, $assertCallback, $query);

        $event->setRunId('rundId2');
        $this->createAndWaitForEvent($event);

        $assertCallback = function ($events) {
            $this->assertCount(3, $events);
        };
        $query = new EventsQueryBuilder();
        $query->setRunId($runId);
        $this->assertEventWithRetries($this->_client, $assertCallback, $query);
    }

    public function testEventsSearch(): void
    {
        $searchString = 'search-' . $this->_client->generateId();

        $event = new Event();
        $event
            ->setComponent('transformation')
            ->setType('info')
            ->setMessage('test - ' . $searchString)
            ->setConfigurationId('myConfig');
        $searchEvent = $this->createAndWaitForEvent($event);

        $event
            ->setComponent('transformation')
            ->setType('info')
            ->setMessage('test -')
            ->setConfigurationId('myConfig');
        $this->createAndWaitForEvent($event);

        $events = $this->_client->listEvents([
            'q' => $searchString,
        ]);

        $this->assertCount(1, $events);
        $this->assertEquals($searchEvent['id'], $events[0]['id']);
        $this->assertEventUuid($events[0]);
    }

    public function testEmptyEventsSearch(): void
    {
        $searchString = 'search-' . $this->_client->generateId();
        $events = $this->_client->listEvents([
            'q' => $searchString,
        ]);

        $this->assertCount(0, $events);
    }

    /**
     * @dataProvider invalidQueries
     * @param $query
     */
    public function testInvalidSearchSyntaxUserError($query): void
    {
        try {
            $this->_client->listEvents([
                'q' => $query,
            ]);
            $this->fail('Query should not be parsed');
        } catch (ClientException $e) {
            $this->assertEquals(400, $e->getCode());
            $this->assertStringStartsWith('Failed to parse query', $e->getMessage());
        }
    }

    public function invalidQueries()
    {
        return [
            'colon as prefix' => [
                ': success',
            ],
            'colon as suffix' => [
                'success:',
            ],
            'slash as prefix' => [
                '/GET',
            ],
            'asterisk as prefix+suffix' => [
                '*tables*', // works fine with ElasticSearch 7
            ],
        ];
    }

    public function testEventListingMaxLimit(): void
    {
        $events = $this->_client->listEvents([
            'limit' => 10000,
        ]);
        $this->assertNotEmpty($events);

        try {
            $this->_client->listEvents([
                'limit' => 10001,
            ]);
            $this->fail('Limit should not be allowed');
        } catch (ClientException $e) {
            $this->assertEquals(400, $e->getCode());
        }
    }
}
