<?php
/**
 *
 * User: Martin Halamíček
 * Date: 16.5.12
 * Time: 11:46
 *
 */


namespace Keboola\Test\Common;

use Keboola\Test\StorageApiTestCase;
use Keboola\StorageApi\Event;

class EventsTest extends StorageApiTestCase
{

    public function testEventCreate()
    {
        $event = new Event();
        $event->setComponent('ex-sfdc')
            ->setConfigurationId('sys.c-sfdc.account-01')
            ->setDuration(200)
            ->setType('info')
            ->setRunId('ddddssss')
            ->setMessage('Table Opportunity fetched.')
            ->setDescription('Some longer description of event')
            ->setParams(array(
                'accountName' => 'Keboola',
                'configuration' => 'sys.c-sfdc.sfdc-01',
            ));

        $savedEvent = $this->createAndWaitForEvent($event);

        $this->assertEquals($event->getComponent(), $savedEvent['component']);
        $this->assertEquals($event->getConfigurationId(), $savedEvent['configurationId']);
        $this->assertEquals($event->getMessage(), $savedEvent['message']);
        $this->assertEquals($event->getDescription(), $savedEvent['description']);
        $this->assertEquals($event->getType(), $savedEvent['type']);
    }

    public function testEventCreateWithoutParams()
    {
        $event = new Event();
        $event->setComponent('ex-sfdc')
            ->setType('info')
            ->setMessage('Table Opportunity fetched.');

        $event = $this->createAndWaitForEvent($event);

        // to check if params is object we have to convert received json to objects instead of assoc array
        // so we have to use raw Http Client
        $client = new \GuzzleHttp\Client([
            'base_uri' => $this->_client->getApiUrl(),
        ]);

        $response = $client->get('/v2/storage/events/' . $event['id'], array(
            'headers' => array(
                'X-StorageApi-Token' => $this->_client->getTokenString(),
            ),
        ));

        $response = json_decode((string)$response->getBody());

        $this->assertInstanceOf('stdclass', $response->params);
        $this->assertInstanceOf('stdclass', $response->results);
        $this->assertInstanceOf('stdclass', $response->performance);
    }

    /**
     * @expectedException \Keboola\StorageApi\Exception
     */
    public function testInvalidType()
    {
        $event = new Event();
        $event->setType('homeless');
    }

    public function testEventCompatibility()
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
    public function testCreateInvalidUTF8()
    {
        $message = "SQLSTATE[XX000]: " . chr(0x00000080);
        $event = new Event();
        $event->setComponent('ex-sfdc')
            ->setType('info')
            ->setMessage($message);

        try {
            $this->createAndWaitForEvent($event);
            $this->fail('event should not be created');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('malformedRequest', $e->getStringCode());
        }
    }

    public function testEventList()
    {
        // at least one event should be generated
        $this->_client->listBuckets();

        $events = $this->_client->listEvents(1);
        $this->assertCount(1, $events);
    }

    public function testEventsFiltering()
    {
        $events = $this->_client->listEvents(array(
            'limit' => 100,
            'offset' => 0
        ));


        $lastEvent = reset($events);
        $lastEventId = $lastEvent['id'];

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

        $events = $this->_client->listEvents(array(
            'sinceId' => $lastEventId,
            'runId' => $runId,
        ));

        $this->assertCount(3, $events);

        $events = $this->_client->listEvents(array(
            'sinceId' => $lastEventId,
            'component' => 'transformation',
        ));
        $this->assertCount(1, $events, 'filter by component');

        $event->setRunId('rundId2');
        $this->createAndWaitForEvent($event);

        $events = $this->_client->listEvents(array(
            'sinceId' => $lastEventId,
            'runId' => $runId,
        ));
        $this->assertCount(3, $events);
    }

    public function testEventsSearch()
    {
        $searchString = 'search-' . $this->_client->generateId();

        $event = new Event();
        $event
            ->setComponent('transformation')
            ->setType('info')
            ->setMessage('test - ' . $searchString)
            ->setConfigurationId('myConfig');
        $searchEvent  = $this->createAndWaitForEvent($event);

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
    }
}
