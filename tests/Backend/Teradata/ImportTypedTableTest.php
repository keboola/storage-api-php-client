<?php

namespace Keboola\Test\Backend\Teradata;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\ClientException;
use Keboola\Test\Backend\Workspaces\ParallelWorkspacesTestCase;

class ImportTypedTableTest extends ParallelWorkspacesTestCase
{
    public function setUp(): void
    {
        parent::setUp();
        $this->initEmptyTestBucketsForParallelTests();
    }

    public function testLoadTypedTablesConversionError(): void
    {
        $fullLoadFile = __DIR__ . '/../../_data/users.csv';
        $bucketId = $this->getTestBucketId();
        $payload = [
            'name' => 'users-types',
            'primaryKeysNames' => ['id'],
            'columns' => [
                ['name' => 'id', 'definition' => ['type' => 'INT', 'nullable' => false]],
                ['name' => 'name', 'definition' => ['type' => 'VARCHAR']],
                ['name' => 'city', 'definition' => ['type' => 'VARCHAR']],
                ['name' => 'sex', 'definition' => ['type' => 'INT']],
            ],
        ];
        $runId = $this->_client->generateRunId();
        $this->_client->setRunId($runId);

        // create table
        $tableId = $this->_client->createTableDefinition($bucketId, $payload);

        // block until async events are processed, processing in order is not guaranteed but it should work most of time
        $this->createAndWaitForEvent((new \Keboola\StorageApi\Event())->setComponent('dummy')->setMessage('dummy'));

        try {
            // try import data with wrong types with full load
            $this->_client->writeTableAsync(
                $tableId,
                new CsvFile($fullLoadFile),
                [
                    'incremental' => false,
                ]
            );
            $this->fail('Should throw ClientException');
        } catch (ClientException $e) {
            self::assertStringContainsString('Load error: Teradata TPT load ended with Error', $e->getMessage());
        }

        try {
            // try import data with wrong types with full load
            $this->_client->writeTableAsync(
                $tableId,
                new CsvFile($fullLoadFile),
                [
                    'incremental' => true,
                ]
            );
            $this->fail('Should throw ClientException');
        } catch (ClientException $e) {
            self::assertStringContainsString('Load error: Teradata TPT load ended with Error', $e->getMessage());
        }
    }
}
