<?php
namespace Keboola\Test\Common;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Options\FileUploadOptions;
use Keboola\Test\StorageApiTestCase;

class QueueJobsTest extends StorageApiTestCase
{
    public function setUp()
    {
        parent::setUp();
        $this->_client->createBucket('test', Client::STAGE_IN);
        $this->_client->createTableAsync('in.c-test', 'table1', new CsvFile(__DIR__ . '/../_data/languages-headers.csv'));
    }

    public function tearDown()
    {
        parent::tearDown();
        $this->_client->dropBucket('in.c-test', ['force' => true]);
    }

    public function testQueueTableImport()
    {
        $fileId = $this->_client->uploadFile(__DIR__ . '/../_data/languages.csv', new FileUploadOptions());
        $jobId = $this->_client->queueTableImport('in.c-test.table1', ['dataFileId' => $fileId]);
        $job = $this->_client->getJob($jobId);
        $this->assertEquals('in.c-test.table1', $job['tableId']);
        $this->assertEquals('tableImport', $job['operationName']);
        $this->assertEquals($fileId, $job['operationParams']['source']['fileId']);
        $this->assertEquals('file', $job['operationParams']['source']['type']);
    }
}
