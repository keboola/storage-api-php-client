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
        $bucketData = array(
            'name' => 'timetravel-test',
            'stage' => 'in',
            'description' => 'time travel test bucket',
        );
        $newBucketId = $this->_client->createBucket(
            $bucketData['name'],
            $bucketData['stage'],
            $bucketData['description']
        );

        $importFile = new CsvFile(__DIR__ . '/../../_data/languages.csv');

        $sourceTable = 'languages_' . date('Ymd_His');

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

        $newTableName = "new-table-name_" . date('Ymd_His', strtotime($timestamp));

        $replicaTableId = $this->_client->createTableFromSourceTableAtTimestamp(
            $newBucketId,
            $sourceTableId,
            $timestamp,
            $newTableName
        );

        $replicaTable = $this->_client->getTable($replicaTableId);

        $this->assertEquals($newTableName, $replicaTable['name']);
        $this->assertEquals($updatedTable['rowsCount'], $replicaTable['rowsCount'] * 2);
        $this->assertEquals($originalTable['rowsCount'], $replicaTable['rowsCount']);
    }

    public function testCreateTableFromTimestampOfAlteredTable()
    {
        $bucketData = array(
            'name' => 'timetravel-test',
            'stage' => 'in',
            'description' => 'time travel test bucket',
        );
        $newBucketId = $this->_client->createBucket(
            $bucketData['name'],
            $bucketData['stage'],
            $bucketData['description']
        );

        $importFile = new CsvFile(__DIR__ . '/../../_data/languages.csv');

        $sourceTable = 'languages_' . date('Ymd_His');

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

        $this->_client->addTableColumn($sourceTableId, "new-column");

        $updatedTable = $this->_client->getTable($sourceTableId);

        $newTableName = "new-table-name_" . date('Ymd_His', strtotime($timestamp));

        $replicaTableId = $this->_client->createTableFromSourceTableAtTimestamp(
            $newBucketId,
            $sourceTableId,
            $timestamp,
            $newTableName
        );

        $replicaTable = $this->_client->getTable($replicaTableId);

        $this->assertEquals($newTableName, $replicaTable['name']);
        $this->assertEquals($originalTable['columns'], $replicaTable['columns']);
        $this->assertGreaterThan(count($updatedTable['columns']), count($replicaTable['columns']));
    }
}
