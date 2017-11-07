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
        $sourceTableId = $this->_client->createTable(
            $this->getTestBucketId(),
            'languages',
            $importFile
        );
        $originalTable = $this->_client->getTable($sourceTableId);
        $timestamp = date(DATE_ATOM);
        sleep(30);

        $this->_client->writeTable($sourceTableId, $importFile, ['incremental' => true]);

        $updatedTable = $this->_client->getTable($sourceTableId);

        $replicaTableId = $this->_client->createTableFromSourceTableAtTimestamp(
            $this->getTestBucketId(),
            $sourceTableId,
            $timestamp,
            "new-table-name"
        );

        $replicaTable = $this->_client->getTable($replicaTableId);

        $this->assertEquals('new-table-name', $replicaTable['name']);
        $this->assertEquals($updatedTable['rowsCount'], $replicaTable['rowsCount'] * 2);
        $this->assertEquals($originalTable['rowsCount'], $replicaTable['rowsCount']);
    }
}
