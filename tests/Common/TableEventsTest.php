<?php

namespace Keboola\Test\Common;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Event;
use Keboola\Test\StorageApiTestCase;

class TableEventsTest extends StorageApiTestCase
{
    public function setUp(): void
    {
        parent::setUp();
        $this->initEmptyTestBucketsForParallelTests();
    }

    public function testTableEvents(): void
    {
        // set lastEventId
        $lastEvent = $this->initEvents($this->_client);
        $importFile = __DIR__ . '/../_data/languages.csv';
        $tableId = $this->_client->createTableAsync(
            $this->getTestBucketId(),
            'languages',
            new CsvFile($importFile),
        );
        // wait for events to be created
        $this->createAndWaitForEvent((new Event())
            ->setComponent('dummy')
            ->setMessage('dummy'), $this->_client);

        // sinceId id (int)
        $events = $this->_client->listTableEvents(
            $tableId,
            [
                'sinceId' => $lastEvent['id'],
            ],
        );
        $this->assertCount(3, $events);
        $this->assertEventUuid($events[0]);

        // sinceId uuid
        $events = $this->_client->listTableEvents(
            $tableId,
            [
                'sinceId' => $lastEvent['uuid'],
            ],
        );
        $this->assertCount(3, $events);
        $this->assertEventUuid($events[0]);
    }
}
