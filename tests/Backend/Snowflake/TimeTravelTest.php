<?php

namespace Keboola\Test\Backend\Snowflake;

use Keboola\Test\StorageApiTestCase;
use Keboola\Csv\CsvFile;

class TimeTravelTest extends StorageApiTestCase
{

    public function setUp()
    {
        parent::setUp();
        $this->_initEmptyTestBuckets();
    }

    public function testCreateTableFromTimestamp()
    {
        $importFile = new CsvFile(__DIR__ . '/../../_data/languages.csv');

        $sourceTable = 'languages_' . date('yyyy-mm-dd_His');

        $sourceTableId = $this->_client->createTable(
            $this->getTestBucketId(),
            $sourceTable,
            $importFile
        );
        $originalTable = $this->_client->getTable($sourceTableId);
        // the timestamp must be at least 1 sec > creation time
        sleep(5);
        $timestamp = date(DATE_ATOM);
        sleep(25);

        $this->_client->writeTable($sourceTableId, $importFile, ['incremental' => true]);

        $updatedTable = $this->_client->getTable($sourceTableId);

        $newTableName = "new-table-name_" . date('yyyy-mm-dd_His', strtotime($timestamp));

        $replicaTableId = $this->_client->createTableFromSourceTableAtTimestamp(
            $this->getTestBucketId(),
            $sourceTableId,
            $timestamp,
            $newTableName
        );

        $replicaTable = $this->_client->getTable($replicaTableId);

        $this->assertEquals($newTableName, $replicaTable['name']);
        $this->assertEquals($updatedTable['rowsCount'], $replicaTable['rowsCount'] * 2);
        $this->assertEquals($originalTable['rowsCount'], $replicaTable['rowsCount']);
    }
}
