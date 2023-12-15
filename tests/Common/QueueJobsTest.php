<?php
namespace Keboola\Test\Common;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Options\FileUploadOptions;
use Keboola\Test\StorageApiTestCase;

class QueueJobsTest extends StorageApiTestCase
{
    public function setUp(): void
    {
        parent::setUp();
        $this->_initEmptyTestBuckets([Client::STAGE_IN]);
        $this->_client->createTableAsync('in.c-API-tests', 'table1', new CsvFile(__DIR__ . '/../_data/languages-headers.csv'));
    }

    public function testQueueTableImportFromFile(): void
    {
        $fileId = $this->_client->uploadFile(__DIR__ . '/../_data/languages.csv', new FileUploadOptions());
        $jobId = $this->_client->queueTableImport('in.c-API-tests.table1', ['dataFileId' => $fileId]);
        $job = $this->_client->getJob($jobId);
        $this->assertEquals('in.c-API-tests.table1', $job['tableId']);
        $this->assertEquals('tableImport', $job['operationName']);
        $this->assertEquals($fileId, $job['operationParams']['source']['fileId']);
        $this->assertEquals('file', $job['operationParams']['source']['type']);
    }

    public function testQueueTableImportFromWorkspace(): void
    {
        $jobId = $this->_client->queueTableImport(
            'in.c-API-tests.table1',
            [
                'dataWorkspaceId' => 1000,
                'dataTableName' => 'myTable',
            ],
        );
        $job = $this->_client->getJob($jobId);
        $this->assertEquals('in.c-API-tests.table1', $job['tableId']);
        $this->assertEquals('tableImport', $job['operationName']);
        $this->assertEquals(1000, $job['operationParams']['source']['workspaceId']);
        $this->assertEquals('myTable', $job['operationParams']['source']['tableName']);
        $this->assertEquals('myTable', $job['operationParams']['source']['dataObject']);
        $this->assertEquals('workspace', $job['operationParams']['source']['type']);
    }

    public function testQueueTableExport(): void
    {
        $jobId = $this->_client->queueTableExport('in.c-API-tests.table1', []);
        $job = $this->_client->getJob($jobId);
        $this->assertEquals('in.c-API-tests.table1', $job['tableId']);
        $this->assertEquals('tableExport', $job['operationName']);
    }

    /**
     * @dataProvider invalidQueueCreateTableOptions
     * @param array $options
     */
    public function testQueueCreateTableInvalidName($options): void
    {
        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('Table create option "name" is required and cannot be empty.');
        $this->_client->queueTableCreate('in.c-API-tests.table1', $options);
    }

    public function invalidQueueCreateTableOptions()
    {
        return [
            'name missing' => [
                [
                    'dataFileId' => 100,
                ],
            ],
            'name is null' => [
                [
                    'dataFileId' => 100,
                    'name' => null,
                ],
            ],
            'name is empty' => [
                [
                    'dataFileId' => 100,
                    'name' => '',
                ],
            ],
        ];
    }

    public function testQueueCreateTableFromFile(): void
    {
        $fileId = $this->_client->uploadFile(__DIR__ . '/../_data/languages.csv', new FileUploadOptions());
        $jobId = $this->_client->queueTableCreate('in.c-API-tests.table1', [
            'dataFileId' => $fileId,
            'name' => 'my-new-queued-table',
        ]);
        $job = $this->_client->getJob($jobId);
        $this->assertNull($job['tableId']);
        $this->assertEquals('tableCreate', $job['operationName']);
        $this->assertEquals($fileId, $job['operationParams']['source']['fileId']);
        $this->assertEquals('file', $job['operationParams']['source']['type']);
    }

    public function testQueueCreateTableFromWorkspace(): void
    {
        $jobId = $this->_client->queueTableCreate(
            'in.c-API-tests.table1',
            [
                'dataWorkspaceId' => 1000,
                'dataTableName' => 'myTable',
                'name' => 'my-new-queued-table',
            ],
        );
        $job = $this->_client->getJob($jobId);
        $this->assertNull($job['tableId']);
        $this->assertEquals('tableCreate', $job['operationName']);
        $this->assertEquals(1000, $job['operationParams']['source']['workspaceId']);
        $this->assertEquals('myTable', $job['operationParams']['source']['tableName']);
        $this->assertEquals('myTable', $job['operationParams']['source']['dataObject']);
        $this->assertEquals('workspace', $job['operationParams']['source']['type']);
    }
}
