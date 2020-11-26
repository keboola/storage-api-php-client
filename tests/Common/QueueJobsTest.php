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
        $this->_initEmptyTestBuckets([Client::STAGE_IN]);
        $this->_client->createTableAsync('in.c-API-tests', 'table1', new CsvFile(__DIR__ . '/../_data/languages-headers.csv'));
    }

    public function testQueueTableImportFromFile()
    {
        $fileId = $this->_client->uploadFile(__DIR__ . '/../_data/languages.csv', new FileUploadOptions());
        $jobId = $this->_client->queueTableImport('in.c-API-tests.table1', ['dataFileId' => $fileId]);
        $job = $this->_client->getJob($jobId);
        $this->assertEquals('in.c-API-tests.table1', $job['tableId']);
        $this->assertEquals('tableImport', $job['operationName']);
        $this->assertEquals($fileId, $job['operationParams']['source']['fileId']);
        $this->assertEquals('file', $job['operationParams']['source']['type']);
    }

    public function testQueueTableImportFromWorkspace()
    {
        $jobId = $this->_client->queueTableImport(
            'in.c-API-tests.table1',
            [
                'dataWorkspaceId' => 'myWorkspace',
                'dataTableName' => 'myTable',
            ]
        );
        $job = $this->_client->getJob($jobId);
        $this->assertEquals('in.c-API-tests.table1', $job['tableId']);
        $this->assertEquals('tableImport', $job['operationName']);
        $this->assertEquals('myWorkspace', $job['operationParams']['source']['workspaceId']);
        $this->assertEquals('myTable', $job['operationParams']['source']['tableName']);
        $this->assertEquals('myTable', $job['operationParams']['source']['dataObject']);
        $this->assertEquals('workspace', $job['operationParams']['source']['type']);
    }

    public function testQueueTableExport()
    {
        $jobId = $this->_client->queueTableExport('in.c-API-tests.table1', []);
        $job = $this->_client->getJob($jobId);
        $this->assertEquals('in.c-API-tests.table1', $job['tableId']);
        $this->assertEquals('tableExport', $job['operationName']);
    }
}
