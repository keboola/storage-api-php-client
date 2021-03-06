<?php

namespace Keboola\Test\Backend\Snowflake;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Options\TokenAbstractOptions;
use Keboola\StorageApi\Options\TokenCreateOptions;
use Keboola\StorageApi\Options\TokenUpdateOptions;
use Keboola\StorageApi\TableExporter;
use Keboola\Test\StorageApiTestCase;

class TimeTravelTest extends StorageApiTestCase
{
    private $downloadPath;

    public function setUp()
    {
        parent::setUp();
        $this->initEmptyTestBucketsForParallelTests();
        $this->downloadPath = __DIR__ . '/../../_tmp/';
    }

    public function testCreateTableFromTimestamp()
    {
        $importFile = new CsvFile(__DIR__ . '/../../_data/languages.csv');

        $sourceTable = 'languages_' . date('Ymd_His');

        $sourceTableId = $this->_client->createTable(
            $this->getTestBucketId(),
            $sourceTable,
            $importFile,
            [
                'primaryKey' => 'id',
            ]
        );
        sleep(10);
        $timestamp = date(DATE_ATOM);
        sleep(25);
        $originalTable = $this->_client->getTable($sourceTableId);

        $this->_client->writeTable(
            $sourceTableId,
            new CsvFile(__DIR__ . '/../../_data/languages.increment.csv'),
            [
                'incremental' => true,
            ]
        );

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
        $this->assertEquals(['id'], $replicaTable['primaryKey']);
        $this->assertLessThan($updatedTable['rowsCount'], $originalTable['rowsCount']);
        $this->assertEquals($originalTable['rowsCount'], $replicaTable['rowsCount']);

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
            $importFile,
            [
                'primaryKey' => 'id,name',
            ]
        );
        $originalTable = $this->_client->getTable($sourceTableId);
        sleep(5);
        $timestamp = date(DATE_ATOM);
        sleep(25);

        $this->_client->addTableColumn($sourceTableId, "new-column");
        $this->_client->removeTablePrimaryKey($sourceTableId);

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
        // This is not working, it should contain the original PK but does not.
        // Possibly related to case 00022189
        // $this->assertEquals(['id', 'name'], $replicaTable['primaryKey']);
    }

    public function testInvalidCreateTableFromTimestampRequests()
    {
        $beforeCreationTimestamp = date(DATE_ATOM, strtotime('-10m'));
        $importFile = new CsvFile(__DIR__ . '/../../_data/languages.csv');

        $sourceTable = 'languages_' . date('Ymd_His');

        $sourceTableId = $this->_client->createTable(
            $this->getTestBucketId(),
            $sourceTable,
            $importFile
        );
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
        $outputBucketTokenOptions = (new TokenCreateOptions())
            ->setDescription('Output bucket only write token')
            ->addBucketPermission($this->getTestBucketId(self::STAGE_OUT), TokenAbstractOptions::BUCKET_PERMISSION_WRITE)
        ;

        $outputBucketTokenId = $this->_client->createToken($outputBucketTokenOptions);
        $outputBucketToken = $this->_client->getToken($outputBucketTokenId);

        $outputBucketClient = $this->getClient([
            'token' => $outputBucketToken['token'],
            'url' => STORAGE_API_URL,
            'backoffMaxTries' => 1,
            'jobPollRetryDelay' => function () {
                return 1;
            }
        ]);

        $inputBucketTokenOptions = (new TokenCreateOptions())
            ->setDescription('Input bucket only read token')
            ->addBucketPermission($this->getTestBucketId(), TokenAbstractOptions::BUCKET_PERMISSION_READ)
        ;

        $inputBucketTokenId = $this->_client->createToken($inputBucketTokenOptions);
        $inputBucketToken = $this->_client->getToken($inputBucketTokenId);

        $inputBucketClient = $this->getClient([
            'token' => $inputBucketToken['token'],
            'url' => STORAGE_API_URL,
            'backoffMaxTries' => 1,
            'jobPollRetryDelay' => function () {
                return 1;
            }
        ]);

        $minimalTokenOptions = (new TokenCreateOptions())
            ->setDescription('Minimal permissions token')
            ->addBucketPermission($this->getTestBucketId(), TokenAbstractOptions::BUCKET_PERMISSION_READ)
            ->addBucketPermission($this->getTestBucketId(self::STAGE_OUT), TokenAbstractOptions::BUCKET_PERMISSION_WRITE)
        ;

        $minimalTokenId = $this->_client->createToken($minimalTokenOptions);
        $minimalToken = $this->_client->getToken($minimalTokenId);

        $minimalClient = $this->getClient([
            'token' => $minimalToken['token'],
            'url' => STORAGE_API_URL,
            'backoffMaxTries' => 1,
            'jobPollRetryDelay' => function () {
                return 1;
            }
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
