<?php

namespace Keboola\Test\Backend\Snowflake;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\TableExporter;
use Keboola\Test\StorageApiTestCase;

class TimeTravelTest extends StorageApiTestCase
{
    private $downloadPath;

    public function setUp()
    {
        parent::setUp();
        $this->_initEmptyTestBuckets();
        $this->downloadPath = __DIR__ . '/../../_tmp/';
    }
    
    public function testCreateTableFromTimestamp()
    {
        $importFile = new CsvFile(__DIR__ . '/../../_data/languages.csv');

        $sourceTable = 'languages_' . date('Ymd_His');
        
        $sourceTableId = $this->_client->createTable(
            $this->getTestBucketId(),
            $sourceTable,
            $importFile
        );
        $originalTable = $this->_client->getTable($sourceTableId);
        // the timestamp must be at least 1 sec > creation time
        sleep(10);
        $timestamp = date(DATE_ATOM);
        sleep(25);

        $this->_client->writeTable($sourceTableId, $importFile, ['incremental' => true]);

        $updatedTable = $this->_client->getTable($sourceTableId);
        $newTableName = "new-table-name_" . date('Ymd_His', strtotime($timestamp));

        $replicaTableId = $this->_client->createTableFromSourceTableAtTimestamp(
            $this->getTestBucketId(self::STAGE_OUT),
            $sourceTableId,
            $timestamp,
            $newTableName
        );

        $replicaTable = $this->_client->getTable($replicaTableId);

        $this->assertEquals($newTableName, $replicaTable['name']);

        // Pending fix of timetravel metadata issue case 00022189
        //
        // $this->assertEquals($updatedTable['rowsCount'], $replicaTable['rowsCount'] * 2);
        // $this->assertEquals($originalTable['rowsCount'], $replicaTable['rowsCount']);

        // test data export
        $exporter = new TableExporter($this->_client);
        $downloadFile = $this->downloadPath . 'timeTravelOutput.csv';
        $exporter->exportTable($replicaTableId, $downloadFile, []);
        $this->assertArrayEqualsSorted(
            Client::parseCsv(file_get_contents($importFile)),
            Client::parseCsv(file_get_contents($downloadFile)),
            'id'
        );
    }

    public function testCreateTableFromTimestampOfAlteredTable()
    {
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
            $this->getTestBucketId(self::STAGE_OUT),
            $sourceTableId,
            $timestamp,
            $newTableName
        );

        $replicaTable = $this->_client->getTable($replicaTableId);

        $this->assertEquals($newTableName, $replicaTable['name']);
        $this->assertEquals($originalTable['columns'], $replicaTable['columns']);
        $this->assertGreaterThan(count($replicaTable['columns']), count($updatedTable['columns']));
    }

    public function testInvalidCreateTableFromTimestampRequests()
    {
        $beforeCreationTimestamp = date(DATE_ATOM);
        $importFile = new CsvFile(__DIR__ . '/../../_data/languages.csv');

        $sourceTable = 'languages_' . date('Ymd_His');

        $sourceTableId = $this->_client->createTable(
            $this->getTestBucketId(),
            $sourceTable,
            $importFile
        );
        $originalTable = $this->_client->getTable($sourceTableId);
        sleep(20);
        try {
            $replicaTableId = $this->_client->createTableFromSourceTableAtTimestamp(
                $this->getTestBucketId(self::STAGE_OUT),
                $sourceTableId,
                $beforeCreationTimestamp,
                'table_should_never_be_created'
            );
            $this->fail('you should not be able to timeTravel to before table creation');
        } catch (ClientException $e) {
            $this->assertEquals('storage.timetravel.invalid', $e->getStringCode());
        }
    }

    public function testTableCreationInLinkedBucket()
    {
        // make the source table
        $originalTableId = $this->_client->createTableAsync(
            $this->getTestBucketId(),
            'original_table',
            new CsvFile(__DIR__ . '/../../_data/pk.simple.csv')
        );

        // share the source bucket
        $this->_client->shareBucket($this->getTestBucketId());
        $token = $this->_client->verifyToken();

        $sharedBuckets = $this->_client->listSharedBuckets();

        // create a linked bucket in the same project
        $selfLinkedBucketId = $this->_client->linkBucket(
            'same-project-link-test',
            self::STAGE_OUT,
            $token['owner']['id'],
            $this->getTestBucketId()
        );
        $timestamp = $timestamp = date(DATE_ATOM);
        sleep(20);
        // now, creating a timeTravel backup of the original_table and it should appear in the linked bucket also
        $replicaTableId = $this->_client->createTableFromSourceTableAtTimestamp(
            $this->getTestBucketId(),
            $originalTableId,
            $timestamp,
            'timestampedBackup'
        );

        $linkedBucket = $this->_client->getBucket($selfLinkedBucketId);

        $tables = $this->_client->listTables($selfLinkedBucketId);
        $linkedTsTableKey = array_search('timestampedBackup', array_column($tables, 'name'));
        $linkedTsTable = $tables[$linkedTsTableKey];
        $this->assertNotNull($linkedTsTable);
        $data = $this->_client->getTableDataPreview($replicaTableId);
        $linkedData = $this->_client->getTableDataPreview($linkedTsTable['id']);
        $this->assertLinesEqualsSorted($data, $linkedData);
    }
}
