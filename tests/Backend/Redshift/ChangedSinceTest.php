<?php

namespace Keboola\Test\Backend\Redshift;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\TableExporter;
use Keboola\Test\StorageApiTestCase;

class ChangedSinceTest extends StorageApiTestCase
{
    public function setUp()
    {
        parent::setUp();
        if ($this->_client->bucketExists('in.c-API-tests')) {
            $this->_client->dropBucket('in.c-API-tests', ['force' => true]);
        }
        $this->_client->createBucket('API-tests', 'in', null, 'redshift');
    }

    public function tearDown()
    {
        parent::tearDown();
        $this->_client->dropBucket('in.c-API-tests', ['force' => true]);
    }

    public function testExportAsync()
    {
        $tableId = 'in.c-API-tests.test';

        // Create table
        $csv = new CsvFile(__DIR__ . '/../../_data/changed-since-base.csv');
        $this->_client->createTableAsync('in.c-API-tests', 'test', $csv);
        sleep(1);

        $tableInfo = $this->_client->getTable($tableId);
        $tableExporter = new TableExporter($this->_client);
        $path = tempnam(sys_get_temp_dir(), 'keboola-export');

        // Export full
        $tableExporter->exportTable($tableId, $path, []);
        $exported = new CsvFile($path);
        self::assertCount(4, $exported);

        // Export since last update, should be empty
        $tableExporter->exportTable($tableId, $path, ['changedSince' => $tableInfo['lastImportDate']]);
        $exported = new CsvFile($path);
        self::assertCount(1, $exported);

        // Import increment
        $csv = new CsvFile(__DIR__ . '/../../_data/changed-since-increment.csv');
        $this->_client->writeTableAsync($tableId, $csv, ['incremental' => true]);
        sleep(1);

        // Export since last update, should contain one new row
        $tableExporter->exportTable($tableId, $path, ['changedSince' => $tableInfo['lastImportDate']]);
        $exported = new CsvFile($path);
        self::assertCount(2, $exported);
    }
}
