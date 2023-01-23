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
use Keboola\Csv\CsvFile;
use Keboola\Test\Utils\EventsBuilder;

class EventsAttachmentTest extends StorageApiTestCase
{

    public function setUp(): void
    {
        parent::setUp();
        $this->_initEmptyTestBuckets();
    }

    public function testImportEventAttachment(): void
    {
        $this->initEvents($this->_client);
        $runId = $this->_client->generateRunId();
        $this->_client->setRunId($runId);

        $importFile = __DIR__ . '/../_data/languages.csv';
        $table1Id = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN), 'languages', new CsvFile($importFile));
        $this->_client->writeTableAsync($table1Id, new CsvFile($importFile));

        $assertCallback = function ($events) {
            $this->assertCount(2, $events);
            $importEvent = $events[1];
            $this->assertEquals('storage.tableImportDone', $importEvent['event']);
            $this->assertCount(1, $importEvent['attachments']);
        };
        $query = new EventsBuilder();
        $query->setEvent('storage.tableImportDone')
            ->setTokenId($this->tokenId)
            ->setRunId($runId);
        $this->assertEventWithRetries($this->_client, $assertCallback, $query);
    }
}
