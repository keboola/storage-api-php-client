<?php
namespace Keboola\Test\Common;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Options\FileUploadOptions;
use Keboola\Test\StorageApiTestCase;

class QueueTableImportTest extends StorageApiTestCase
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
        $job = $this->_client->queueTableImport('in.c-test.table1', ['dataFileId' => $fileId]);
        $this->assertArrayHasKey('id', $job);
        $this->assertArrayHasKey('status', $job);
        $this->assertEquals('waiting', $job['status']);
        $this->assertArrayHasKey('url', $job);
        $this->assertArrayHasKey('tableId', $job);
        $this->assertEquals('in.c-test.table1', $job['tableId']);
        $this->assertArrayHasKey('operationName', $job);
        $this->assertEquals('tableImport', $job['operationName']);
        $this->assertArrayHasKey('operationParams', $job);
        $this->assertArrayHasKey('params', $job['operationParams']);
        $this->assertArrayHasKey('source', $job['operationParams']);
        $this->assertArrayHasKey('fileId', $job['operationParams']['source']);
        $this->assertArrayHasKey('type', $job['operationParams']['source']);
        $this->assertEquals($fileId, $job['operationParams']['source']['fileId']);
        $this->assertEquals('file', $job['operationParams']['source']['type']);
    }
}
