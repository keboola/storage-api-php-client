<?php

namespace Keboola\Test\Backend\CommonPart1;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Metadata;
use Keboola\StorageApi\Options\TokenAbstractOptions;
use Keboola\StorageApi\Options\TokenCreateOptions;
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

    public function testCreateTypedTableFromTimestamp(): void
    {
        $tokenData = $this->_client->verifyToken();
        $defaultBackend = $tokenData['owner']['defaultBackend'];

        if ($defaultBackend === self::BACKEND_SNOWFLAKE) {
            $columnFloat = 'FLOAT';
            $columnBoolean = 'BOOLEAN';
        } else {
            $columnFloat = 'FLOAT64';
            $columnBoolean = 'BOOL';
        }

        $sourceTable = 'languages_' . date('Ymd_His');
        $tableDefinition = [
            'name' => $sourceTable,
            'primaryKeysNames' => [],
            'columns' => [
                [
                    'name' => 'id',
                    'definition' => [
                        'type' => 'INTEGER',
                        'nullable' => false,
                    ],
                ],
                [
                    'name' => 'column_decimal',
                    'definition' => [
                        'type' => 'DECIMAL',
                        'length' => '4,3',
                    ],
                ],
                [
                    'name' => 'column_float',
                    'definition' => [
                        'type' => $columnFloat,
                    ],
                ],
                [
                    'name' => 'column_boolean',
                    'definition' => [
                        'type' => $columnBoolean,
                    ],
                ],
                [
                    'name' => 'column_date',
                    'definition' => [
                        'type' => 'DATE',
                    ],
                ],
                [
                    'name' => 'column_timestamp',
                    'definition' => [
                        'type' => 'TIMESTAMP',
                    ],
                ],
                [
                    'name' => 'column_varchar',
                    'definition' => [
                        'type' => 'STRING',
                    ],
                ],
            ],
        ];

        $csvFile = $this->createTempCsv();
        $csvFile->writeRow([
            'id',
            'column_decimal',
            'column_float',
            'column_boolean',
            'column_date',
            'column_timestamp',
            'column_varchar',
        ]);
        $csvFile->writeRow(
            [
                '1',
                '003.123',
                '3.14',
                0,
                '1989-08-31',
                '1989-08-31 00:00:00.000',
                'roman',
            ],
        );

        $sourceTableId = $this->_client->createTableDefinition($this->getTestBucketId(), $tableDefinition);

        $this->_client->writeTableAsync($sourceTableId, $csvFile);
        $originalTable = $this->_client->getTable($sourceTableId);

        sleep(10);
        $timestamp = date(DATE_ATOM);
        sleep(25);

        $csvFile = $this->createTempCsv();
        $csvFile->writeRow([
            'id',
            'column_decimal',
            'column_float',
            'column_boolean',
            'column_date',
            'column_timestamp',
            'column_varchar',
        ]);
        $csvFile->writeRow([
            '2',
            '003.123',
            '3.14',
            0,
            '1989-08-31',
            '1989-08-31 00:00:00.000',
            'roman',
        ]);
        $csvFile->writeRow(
            [
                '3',
                '003.123',
                '3.14',
                0,
                '1989-08-31',
                '1989-08-31 00:00:00.000',
                'roman',
            ],
        );

        $this->_client->writeTableAsync($sourceTableId, $csvFile, ['incremental' => true]);
        $newTableName = 'new-table-name_' . date('Ymd_His', (int) strtotime($timestamp));

        $updatedTable = $this->_client->getTable($sourceTableId);

        $replicaTableId = $this->_client->createTableFromSourceTableAtTimestamp(
            $this->getTestBucketId(self::STAGE_OUT),
            $sourceTableId,
            $timestamp,
            $newTableName,
        );

        $replicaTable = $this->_client->getTable($replicaTableId);

        $this->assertEquals($newTableName, $replicaTable['name']);
        $this->assertEquals([], $replicaTable['primaryKey']); // BQ doesn't support pk som this should be empty
        $this->assertLessThan($updatedTable['rowsCount'], $originalTable['rowsCount']);
        $this->assertEquals($originalTable['rowsCount'], $replicaTable['rowsCount']);

        $metadata = reset($replicaTable['metadata']);
        $this->assertSame('storage', $metadata['provider']);
        $this->assertSame('KBC.dataTypesEnabled', $metadata['key']);
        $this->assertSame('true', $metadata['value']);
        $this->assertTrue($replicaTable['isTyped']);

        // check that the new table has correct datypes in metadata
        $metadataClient = new Metadata($this->_client);
        $idColumnMetadata = $metadataClient->listColumnMetadata("{$replicaTableId}.id");
        $decimalColumnMetadata = $metadataClient->listColumnMetadata("{$replicaTableId}.column_decimal");
        $floatColumnMetadata = $metadataClient->listColumnMetadata("{$replicaTableId}.column_float");
        $booleanColumnMetadata = $metadataClient->listColumnMetadata("{$replicaTableId}.column_boolean");
        $dateColumnMetadata = $metadataClient->listColumnMetadata("{$replicaTableId}.column_date");
        $timestampColumnMetadata = $metadataClient->listColumnMetadata("{$replicaTableId}.column_timestamp");
        $varcharColumnMetadata = $metadataClient->listColumnMetadata("{$replicaTableId}.column_varchar");

        if ($defaultBackend === self::BACKEND_SNOWFLAKE) {
            $columnInteger = 'NUMBER';
            $columnNumeric = 'NUMBER';
            $columnFloat = 'FLOAT';
            $columnBoolean = 'BOOLEAN';
            $columnTimestamp = 'TIMESTAMP_NTZ';
            $columnString = 'VARCHAR';
        } else {
            $columnInteger = 'INTEGER';
            $columnNumeric = 'NUMERIC';
            $columnFloat = 'FLOAT64';
            $columnBoolean = 'BOOL';
            $columnTimestamp = 'TIMESTAMP';
            $columnString = 'STRING';
        }

        $this->assertArrayEqualsExceptKeys([
            'key' => 'KBC.datatype.type',
            'value' => $columnInteger,
            'provider' => 'storage',
        ], $idColumnMetadata[0], ['id', 'timestamp']);
        $this->assertArrayEqualsExceptKeys([
            'key' => 'KBC.datatype.type',
            'value' => $columnNumeric,
            'provider' => 'storage',
        ], $decimalColumnMetadata[0], ['id', 'timestamp']);
        $this->assertArrayEqualsExceptKeys([
            'key' => 'KBC.datatype.type',
            'value' => $columnFloat,
            'provider' => 'storage',
        ], $floatColumnMetadata[0], ['id', 'timestamp']);
        $this->assertArrayEqualsExceptKeys([
            'key' => 'KBC.datatype.type',
            'value' => $columnBoolean,
            'provider' => 'storage',
        ], $booleanColumnMetadata[0], ['id', 'timestamp']);
        $this->assertArrayEqualsExceptKeys([
            'key' => 'KBC.datatype.type',
            'value' => 'DATE',
            'provider' => 'storage',
        ], $dateColumnMetadata[0], ['id', 'timestamp']);
        $this->assertArrayEqualsExceptKeys([
            'key' => 'KBC.datatype.type',
            'value' => $columnTimestamp,
            'provider' => 'storage',
        ], $timestampColumnMetadata[0], ['id', 'timestamp']);
        $this->assertArrayEqualsExceptKeys([
            'key' => 'KBC.datatype.type',
            'value' => $columnString,
            'provider' => 'storage',
        ], $varcharColumnMetadata[0], ['id', 'timestamp']);
    }

    /**
     * @group global-search
     */
    public function testCreateTableFromTimestamp(): void
    {
        $importFile = new CsvFile(__DIR__ . '/../../_data/languages.csv');

        $sourceTable = 'languages_' . date('Ymd_His');

        $sourceTableId = $this->_client->createTableAsync(
            $this->getTestBucketId(),
            $sourceTable,
            $importFile,
            [
                'primaryKey' => 'id',
            ],
        );
        sleep(10);
        $timestamp = date(DATE_ATOM);
        sleep(25);
        $originalTable = $this->_client->getTable($sourceTableId);

        $this->_client->writeTableAsync(
            $sourceTableId,
            new CsvFile(__DIR__ . '/../../_data/languages.increment.csv'),
            [
                'incremental' => true,
            ],
        );

        $updatedTable = $this->_client->getTable($sourceTableId);

        $newTableName = 'new-table-name_' . date('Ymd_His', (int) strtotime($timestamp));

        $hashedUniqueTableName = sha1($newTableName . '-'.$this->generateDescriptionForTestObject());
        $replicaTableId = $this->_client->createTableFromSourceTableAtTimestamp(
            $this->getTestBucketId(self::STAGE_OUT),
            $sourceTableId,
            $timestamp,
            $hashedUniqueTableName,
        );

        $apiCall = fn() => $this->_client->globalSearch($hashedUniqueTableName);
        $assertCallback = function ($searchResult) use ($hashedUniqueTableName) {
            $this->assertSame(1, $searchResult['all']);
            $this->assertSame('table', $searchResult['items'][0]['type']);
            $this->assertSame($hashedUniqueTableName, $searchResult['items'][0]['name']);
        };
        $this->retryWithCallback($apiCall, $assertCallback);

        $replicaTable = $this->_client->getTable($replicaTableId);

        $this->assertEquals($hashedUniqueTableName, $replicaTable['name']);
        if ($this->getDefaultBackend($this->_client) === self::BACKEND_BIGQUERY) {
            $this->assertEquals([], $replicaTable['primaryKey']);
        } else {
            $this->assertEquals(['id'], $replicaTable['primaryKey']);
        }
        $this->assertLessThan($updatedTable['rowsCount'], $originalTable['rowsCount']);
        $this->assertEquals($originalTable['rowsCount'], $replicaTable['rowsCount']);

        // test data export
        $exporter = new TableExporter($this->_client);
        $downloadFile = $this->downloadPath . 'timeTravelOutput.csv';
        $exporter->exportTable($replicaTableId, $downloadFile, []);

        /** @var string $downloadedFileContent */
        $downloadedFileContent = file_get_contents($downloadFile);
        /** @var string $importedFileContent */
        $importedFileContent = file_get_contents($importFile);
        $this->assertArrayEqualsSorted(
            Client::parseCsv($importedFileContent),
            Client::parseCsv($downloadedFileContent),
            'id',
        );
    }

    public function testCreateTableFromTimestampOfAlteredTable(): void
    {
        $importFile = new CsvFile(__DIR__ . '/../../_data/languages.csv');

        $sourceTable = 'languages_' . date('Ymd_His');

        $sourceTableId = $this->_client->createTableAsync(
            $this->getTestBucketId(),
            $sourceTable,
            $importFile,
            [
                'primaryKey' => 'id,name',
            ],
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
            $newTableName,
        );

        $replicaTable = $this->_client->getTable($replicaTableId);

        $this->assertEquals($newTableName, $replicaTable['name']);
        $this->assertEquals($originalTable['columns'], $replicaTable['columns']);
        $this->assertGreaterThan(count($replicaTable['columns']), count($updatedTable['columns']));
        // This is not working, it should contain the original PK but does not.
        // Possibly related to case 00022189
        // $this->assertEquals(['id', 'name'], $replicaTable['primaryKey']);
    }

    public function testInvalidCreateTableFromTimestampRequests(): void
    {
        $beforeCreationTimestamp = date(DATE_ATOM, strtotime('-10m'));
        $importFile = new CsvFile(__DIR__ . '/../../_data/languages.csv');

        $sourceTable = 'languages_' . date('Ymd_His');

        $sourceTableId = $this->_client->createTableAsync(
            $this->getTestBucketId(),
            $sourceTable,
            $importFile,
        );
        try {
            $this->_client->createTableFromSourceTableAtTimestamp(
                $this->getTestBucketId(self::STAGE_OUT),
                $sourceTableId,
                $beforeCreationTimestamp,
                'table_should_never_be_created',
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
            new CsvFile(__DIR__ . '/../../_data/pk.simple.csv'),
        );

        // share the source bucket
        $this->_client->shareBucket($this->getTestBucketId());
        $token = $this->_client->verifyToken();

        // create a linked bucket in the same project
        /** @var string $selfLinkedBucketId */
        $selfLinkedBucketId = $this->_client->linkBucket(
            sha1($this->generateDescriptionForTestObject()) . '-same-project-link-test',
            self::STAGE_OUT,
            $token['owner']['id'],
            $this->getTestBucketId(),
        );
        $timestamp = $timestamp = date(DATE_ATOM);
        sleep(10);
        // now, creating a timeTravel backup of the original_table and it should appear in the linked bucket also
        $replicaTableId = $this->_client->createTableFromSourceTableAtTimestamp(
            $this->getTestBucketId(),
            $originalTableId,
            $timestamp,
            'timestampedBackup',
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
            new CsvFile(__DIR__ . '/../../_data/pk.simple.csv'),
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
                'shouldFail',
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
                'shouldFail',
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
            'created-with-minimal-permission',
        );

        $this->assertNotNull($repllicaTableId);
    }
}
