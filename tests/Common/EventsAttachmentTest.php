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

class EventsAttachmentTest extends StorageApiTestCase
{

    public function setUp()
    {
        parent::setUp();
        $this->_initEmptyTestBuckets();
    }

    public function testImportEventAttachment()
    {
        $runId = $this->_client->generateRunId();
        $this->_client->setRunId($runId);

        $importFile = __DIR__ . '/../_data/languages.csv';
        $table1Id = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN), 'languages', new CsvFile($importFile));
        $this->_client->writeTableAsync($table1Id, new CsvFile($importFile));


        // block until async events are processed, processing in order is not guaranteed but it should work most of time
        $this->createAndWaitForEvent((new \Keboola\StorageApi\Event())->setComponent('dummy')->setMessage('dummy'));

        $events = $this->_client->listEvents([
            'runId' => $runId,
        ]);

        $importEvent = $events[1];
        $this->assertEquals('storage.tableImportDone', $importEvent['event']);
        $this->assertCount(1, $importEvent['attachments']);
    }
}
