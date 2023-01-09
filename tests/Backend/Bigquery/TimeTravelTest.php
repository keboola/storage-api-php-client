<?php

namespace Keboola\Test\Backend\Bigquery;

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
    private string $downloadPath;

    public function setUp(): void
    {
        parent::setUp();
        $this->initEmptyTestBucketsForParallelTests();
        $this->downloadPath = __DIR__ . '/../../_tmp/';
    }

    public function testCreateTableFromTimestamp(): void
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

        $newTableName = 'new-table-name_' . date('Ymd_His', (int) strtotime($timestamp));

        $replicaTableId = $this->_client->createTableFromSourceTableAtTimestamp(
            $this->getTestBucketId(self::STAGE_OUT),
            $sourceTableId,
            $timestamp,
            $newTableName
        );

        $replicaTable = $this->_client->getTable($replicaTableId);

        $this->assertEquals($newTableName, $replicaTable['name']);
        $this->assertEquals([], $replicaTable['primaryKey']); // BQ doesn't support pk som this should be empty
        $this->assertLessThan($updatedTable['rowsCount'], $originalTable['rowsCount']);
        $this->assertEquals($originalTable['rowsCount'], $replicaTable['rowsCount']);

        // test data export
        $exporter = new TableExporter($this->_client);
        $downloadFile = $this->downloadPath . 'timeTravelOutput.csv';
        $exporter->exportTable($replicaTableId, $downloadFile, []);
        /** @var string $downloadFileContent */
        $downloadFileContent = file_get_contents($downloadFile);
        /** @var string $importFileContent */
        $importFileContent = file_get_contents($importFile);
        $this->assertArrayEqualsSorted(
            Client::parseCsv($importFileContent),
            Client::parseCsv($downloadFileContent),
            'id'
        );
    }

    public function testCreateTableFromTimestampOfAlteredTable(): void
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

        $this->_client->addTableColumn($sourceTableId, 'new-column');
        $this->_client->removeTablePrimaryKey($sourceTableId);

        $updatedTable = $this->_client->getTable($sourceTableId);

        $newTableName = 'new-table-name_' . date('Ymd_His', (int) strtotime($timestamp));

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

    public function testInvalidCreateTableFromTimestampRequests(): void
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

    public function testTableCreationInLinkedBucket(): void
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
        /** @var string $selfLinkedBucketId */
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

    public function testTimeTravelBucketPermissions(): void
    {
        // Create the source table
        $originalTableId = $this->_client->createTableAsync(
            $this->getTestBucketId(),
            'original_table',
            new CsvFile(__DIR__ . '/../../_data/pk.simple.csv')
        );
        sleep(10);
        $timestamp = date(DATE_ATOM);
        sleep(10);

        // Setup our test clients
        $outputBucketTokenOptions = (new TokenCreateOptions())
            ->setDescription('Output bucket only write token')
            ->addBucketPermission($this->getTestBucketId(self::STAGE_OUT), TokenAbstractOptions::BUCKET_PERMISSION_WRITE)
        ;

        $outputBucketToken = $this->tokens->createToken($outputBucketTokenOptions);

        $outputBucketClient = $this->getClient([
            'token' => $outputBucketToken['token'],
            'url' => STORAGE_API_URL,
            'backoffMaxTries' => 1,
            'jobPollRetryDelay' => function () {
                return 1;
            },
        ]);

        $inputBucketTokenOptions = (new TokenCreateOptions())
            ->setDescription('Input bucket only read token')
            ->addBucketPermission($this->getTestBucketId(), TokenAbstractOptions::BUCKET_PERMISSION_READ)
        ;

        $inputBucketToken = $this->tokens->createToken($inputBucketTokenOptions);

        $inputBucketClient = $this->getClient([
            'token' => $inputBucketToken['token'],
            'url' => STORAGE_API_URL,
            'backoffMaxTries' => 1,
            'jobPollRetryDelay' => function () {
                return 1;
            },
        ]);

        $minimalTokenOptions = (new TokenCreateOptions())
            ->setDescription('Minimal permissions token')
            ->addBucketPermission($this->getTestBucketId(), TokenAbstractOptions::BUCKET_PERMISSION_READ)
            ->addBucketPermission($this->getTestBucketId(self::STAGE_OUT), TokenAbstractOptions::BUCKET_PERMISSION_WRITE)
        ;

        $minimalToken = $this->tokens->createToken($minimalTokenOptions);

        $minimalClient = $this->getClient([
            'token' => $minimalToken['token'],
            'url' => STORAGE_API_URL,
            'backoffMaxTries' => 1,
            'jobPollRetryDelay' => function () {
                return 1;
            },
        ]);

        // test that only output bucket permissions will fail
        try {
            $outputBucketClient->createTableFromSourceTableAtTimestamp(
                $this->getTestBucketId(self::STAGE_OUT),
                $originalTableId,
                $timestamp,
                'shouldFail'
            );
            $this->fail('No read permission on source bucket');
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
            $this->fail('No write permission on destination bucket');
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
