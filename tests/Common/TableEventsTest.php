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
        $this->initEvents($this->_client);
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
        $events = $this->_client->listTableEvents(
            $tableId,
            [
                'sinceId' => $this->lastEventId,
            ],
        );
        $this->assertCount(3, $events);
    }
}
