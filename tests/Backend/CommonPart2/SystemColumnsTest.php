<?php
/**
 * Created by PhpStorm.
 * User: martinhalamicek
 * Date: 23/09/15
 * Time: 15:39
 */

namespace Keboola\Test\Backend\CommonPart2;

use Keboola\Test\StorageApiTestCase;

class SystemColumnsTest extends StorageApiTestCase
{
    public function setUp(): void
    {
        parent::setUp();
        $this->initEmptyTestBucketsForParallelTests();
    }

    public function testSystemColumnsConversionOnTableCreate(): void
    {
        $excpectedColumns = [
            'id',
            'oid_',
            'tableoid_',
            'xmin_',
            'cmin_',
            'xMax_',
            'cmax_',
            'ctid_',
        ];

        $csvFile = new \Keboola\Csv\CsvFile(__DIR__ . '/../../_data/system-columns.csv');

        // sync
        $tableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN), 'system', $csvFile);
        $table = $this->_client->getTable($tableId);
        $this->assertEquals($excpectedColumns, $table['columns']);

        // async
        $tableId = $this->_client->createTableAsync($this->getTestBucketId(self::STAGE_IN), 'system-async', $csvFile);
        $table = $this->_client->getTable($tableId);
        $this->assertEquals($excpectedColumns, $table['columns']);
    }

    public function testSystemColumnAdd(): void
    {
        $this->skipTestForBackend([
            self::BACKEND_TERADATA,
            self::BACKEND_BIGQUERY
        ],'Column add not supported');

        $csvFile = new \Keboola\Csv\CsvFile(__DIR__ . '/../../_data/languages.csv');

        $tableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN), 'system', $csvFile);
        $this->_client->addTableColumn($tableId, 'oid');
        $this->_client->addTableColumn($tableId, 'CTID');
        $table = $this->_client->getTable($tableId);

        $expectedColumns = [
            'id',
            'name',
            'oid_',
            'CTID_',
        ];
        $this->assertEquals($expectedColumns, $table['columns']);
    }

    public function testSystemColumnImport(): void
    {
        $csvFile = new \Keboola\Csv\CsvFile(__DIR__ . '/../../_data/system-columns.csv');
        $tableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN), 'system', $csvFile);

        $csvImportFile = new \Keboola\Csv\CsvFile(__DIR__ . '/../../_data/system-columns-sanitized.csv');

        $result = $this->_client->writeTable($tableId, $csvImportFile);
        $excpectedColumns = [
            'id',
            'oid_',
            'tableoid_',
            'xmin_',
            'cmin_',
            'xMax_',
            'cmax_',
            'ctid_',
        ];
        $this->assertEquals($excpectedColumns, $result['importedColumns']);
    }

    public function testImportWithNewSystemColumn(): void
    {
        $this->skipTestForBackend([
            self::BACKEND_TERADATA,
            self::BACKEND_BIGQUERY
        ],'Column add not supported');

        $csvFile = new \Keboola\Csv\CsvFile(__DIR__ . '/../../_data/languages.csv');
        $tableId = $this->_client->createTable($this->getTestBucketId(), 'system', $csvFile);

        $importCsvFile = new \Keboola\Csv\CsvFile(__DIR__ . '/../../_data/system-column-added.csv');
        $this->_client->writeTable($tableId, $importCsvFile);

        $table = $this->_client->getTable($tableId);
        $expectedColumns = [
            'id',
            'name',
            'oid_',
        ];
        $this->assertEquals($expectedColumns, $table['columns']);
    }
}
