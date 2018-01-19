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
        // the timestamp must be at least 1 sec > creation time
        sleep(10);
        $timestamp = date(DATE_ATOM);
        sleep(25);

        $this->_client->writeTable($sourceTableId, $importFile, ['incremental' => true]);

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
        sleep(20);
        try {
            $this->_client->createTableFromSourceTableAtTimestamp(
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

        // create a linked bucket in the same project
        $selfLinkedBucketId = $this->_client->linkBucket(
            'same-project-link-test',
            self::STAGE_OUT,
            $token['owner']['id'],
            $this->getTestBucketId()
        );
        $timestamp = $timestamp = date(DATE_ATOM);
        sleep(10);
        // now, creating a timeTravel backup of the original_table and it should appear in the linked bucket also
        $replicaTableId = $this->_client->createTableFromSourceTableAtTimestamp(
            $this->getTestBucketId(),
            $originalTableId,
            $timestamp,
            'timestampedBackup'
        );

        $tables = $this->_client->listTables($selfLinkedBucketId);
        $linkedTsTableKey = array_search('timestampedBackup', array_column($tables, 'name'));
        $linkedTsTable = $tables[$linkedTsTableKey];
        $this->assertNotNull($linkedTsTable);
        $data = $this->_client->getTableDataPreview($replicaTableId);
        $linkedData = $this->_client->getTableDataPreview($linkedTsTable['id']);
        $this->assertLinesEqualsSorted($data, $linkedData);
    }

    public function testTimeTravelBucketPermissions()
    {
        // Create the source table
        $originalTableId = $this->_client->createTableAsync(
            $this->getTestBucketId(),
            'original_table',
            new CsvFile(__DIR__ . '/../../_data/pk.simple.csv')
        );
        sleep(10);
        $timestamp = $timestamp = date(DATE_ATOM);
        sleep(10);

        // Setup our test clients
        $description = 'Output bucket only write token';
        $bucketPermissions = array(
            $this->getTestBucketId(self::STAGE_OUT) => 'write',
        );
        $outputBucketTokenId = $this->_client->createToken($bucketPermissions, $description);
        $outputBucketToken = $this->_client->getToken($outputBucketTokenId);
        $outputBucketClient = new Client([
            'token' => $outputBucketToken['token'],
            'url' => STORAGE_API_URL,
            'backoffMaxTries' => 1,
            'maxJobPollWaitPeriodSeconds' => 1,
        ]);

        $description = 'Input bucket only read token';
        $bucketPermissions = array(
            $this->getTestBucketId() => 'read',
        );
        $inputBucketTokenId = $this->_client->createToken($bucketPermissions, $description);
        $inputBucketToken = $this->_client->getToken($inputBucketTokenId);
        $inputBucketClient = new Client([
            'token' => $inputBucketToken['token'],
            'url' => STORAGE_API_URL,
            'backoffMaxTries' => 1,
            'maxJobPollWaitPeriodSeconds' => 1,
        ]);

        $description = 'Minimal permissions token';
        $bucketPermissions = array(
            $this->getTestBucketId() => 'read',
            $this->getTestBucketId(self::STAGE_OUT) => 'write',
        );
        $minimalTokenId = $this->_client->createToken($bucketPermissions, $description);
        $minimalToken = $this->_client->getToken($minimalTokenId);
        $minimalClient = new Client([
            'token' => $minimalToken['token'],
            'url' => STORAGE_API_URL,
            'backoffMaxTries' => 1,
            'maxJobPollWaitPeriodSeconds' => 1,
        ]);

        // test that only output bucket permissions will fail
        try {
            $outputBucketClient->createTableFromSourceTableAtTimestamp(
                $this->getTestBucketId(self::STAGE_OUT),
                $originalTableId,
                $timestamp,
                'shouldFail'
            );
            $this->fail("No read permission on source bucket");
        } catch (ClientException $e) {
            $this->assertEquals(403, $e->getCode());
        }

        // test that only input bucket permissions will fail
        try {
            $inputBucketClient->createTableFromSourceTableAtTimestamp(
                $this->getTestBucketId(self::STAGE_OUT),
                $originalTableId,
                $timestamp,
                'shouldFail'
            );
            $this->fail("No write permission on destination bucket");
        } catch (ClientException $e) {
            $this->assertEquals(403, $e->getCode());
        }

        // test that minimal permissions will pass
        $repllicaTableId = $minimalClient->createTableFromSourceTableAtTimestamp(
            $this->getTestBucketId(self::STAGE_OUT),
            $originalTableId,
            $timestamp,
            'created-with-minimal-permission'
        );

        $this->assertNotNull($repllicaTableId);
    }
}
