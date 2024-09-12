<?php
/**
 * Created by JetBrains PhpStorm.
 * User: martinhalamicek
 * Date: 22/05/14
 * Time: 16:38
 * To change this template use File | Settings | File Templates.
 */

namespace Keboola\Test\Backend\CommonPart1;

use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Options\FileUploadOptions;
use Keboola\Test\StorageApiTestCase;
use Keboola\Csv\CsvFile;

class CreateTableTest extends StorageApiTestCase
{


    public function setUp(): void
    {
        parent::setUp();
        $this->initEmptyTestBucketsForParallelTests();
    }

    public function testLoadWithInvalidCSVColumns(): void
    {
        if ($this->getDefaultBackend($this->_client) === self::BACKEND_SYNAPSE
        || $this->getDefaultBackend($this->_client) === self::BACKEND_BIGQUERY
        ) {
            $this->markTestSkipped("Synapse and Bigquery don't fail on invalid data");
        }

        $this->expectExceptionMessageMatches('/Load error:*/m');
        /*
         * full exception is :
         * Load error: An exception occurred while executing a query: Number of columns in file (4) does not match that of the corresponding table (2), use file format option error_on_column_count_mismatch=false to ignore this error
  File 'exp-15/19/tables/in/c-API-tests-407341178f0c63f1efa743492408a80d3c9f372b/tableWithInvalidData/3556.csv', line 3, character 1
  Row 1 starts at line 2, column ""__temp_csvimport62dfd37c097aa1_81969579""[4]
  If you would like to continue loading when an error is encountered, use other values such as 'SKIP_FILE' or 'CONTINUE' for the ON_ER
         */
        $this->_client->createTableAsync(
            $this->getTestBucketId(self::STAGE_IN),
            'tableWithInvalidData',
            new CsvFile(__DIR__ . '/../../_data/languages.invalid-data.csv'),
        );
    }

    /**
     * @dataProvider tableCreateData
     * @group global-search
     */
    public function testTableCreate(string $tableName, string $createFile, string $expectationFile, bool $async, array $options = []): void
    {
        if ($tableName === '1' && $this->getDefaultBackend($this->_client) === self::BACKEND_BIGQUERY) {
            $this->markTestSkipped('Bigquery doesn\'t support number as column name');
        }
        if ($async === false) {
            $this->allowTestForBackendsOnly(
                [self::BACKEND_SNOWFLAKE],
                'Test sync actions only on Snowflake',
            );
        }

        $hashedUniqueTableName = sha1($tableName . '-'.$this->generateDescriptionForTestObject());
        $createMethod = $async ? 'createTableAsync' : 'createTable';
        $tableId = $this->_client->{$createMethod}(
            $this->getTestBucketId(self::STAGE_IN),
            $hashedUniqueTableName,
            new CsvFile($createFile),
            $options
        );
        $table = $this->_client->getTable($tableId);

        $apiCall = fn() => $this->_client->globalSearch($hashedUniqueTableName);
        $assertCallback = function ($searchResult) use ($hashedUniqueTableName) {
            $this->assertSame(1, $searchResult['all'], 'GlobalSearch');
            $this->assertSame('table', $searchResult['items'][0]['type'], 'GlobalSearch');
            $this->assertSame($hashedUniqueTableName, $searchResult['items'][0]['name'], 'GlobalSearch');
        };
        $this->retryWithCallback($apiCall, $assertCallback);

        $this->assertArrayHasKey('displayName', $table['bucket']);

        $expectationFileCsv = new CsvFile($expectationFile);

        $this->assertEquals($tableId, $table['id']);
        $this->assertEquals($hashedUniqueTableName, $table['name']);
        $this->assertEquals($hashedUniqueTableName, $table['displayName'], 'display name is same as name');
        $this->assertNotEmpty($table['created']);
        $this->assertNotEmpty($table['lastChangeDate']);
        $this->assertNotEmpty($table['lastImportDate']);
        $this->assertEquals($expectationFileCsv->getHeader(), $table['columns']);
        $this->assertNotEquals('0000-00-00 00:00:00', $table['created']);
        $this->assertNotEmpty($table['dataSizeBytes']);

        $this->assertLinesEqualsSorted(
            file_get_contents($expectationFile),
            $this->_client->getTableDataPreview($tableId),
            'initial data imported into table',
        );
        $displayName = 'Romanov-display-name';
        $hashedDisplayNameName = sha1($displayName . '-'.$this->generateDescriptionForTestObject());
        $tableId = $this->_client->updateTable(
            $tableId,
            [
                'displayName' => $hashedDisplayNameName,
            ],
        );
        assert($tableId !== null);

        $apiCall = fn() => $this->_client->globalSearch($hashedDisplayNameName);
        $assertCallback = function ($searchResult) use ($hashedDisplayNameName) {
            $this->assertSame(1, $searchResult['all'], 'GlobalSearch');
            $this->assertSame('table', $searchResult['items'][0]['type'], 'GlobalSearch');
            $this->assertSame($hashedDisplayNameName, $searchResult['items'][0]['name'], 'GlobalSearch');
        };
        $this->retryWithCallback($apiCall, $assertCallback);

        $table = $this->_client->getTable($tableId);

        $this->assertEquals($hashedDisplayNameName, $table['displayName']);

        // rename table to same name it already has should succeed
        $this->_client->updateTable(
            $tableId,
            [
                'displayName' => $displayName,
            ],
        );

        try {
            $tableId = $this->_client->{$createMethod}(
                $this->getTestBucketId(self::STAGE_IN),
                $displayName,
                new CsvFile($createFile),
                $options
            );
            $this->fail('Should fail');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals(
                sprintf(
                    'The table "%s" in the bucket already has the same display name "%s".',
                    $table['name'],
                    $displayName,
                ),
                $e->getMessage(),
            );
            $this->assertEquals('storage.buckets.tableAlreadyExists', $e->getStringCode());
        }

        try {
            $this->_client->updateTable(
                $tableId,
                [
                    'displayName' => '_wrong-display-name',
                ],
            );
            $this->fail('Should fail');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals(
                'Invalid data - displayName: \'_wrong-display-name\' contains not allowed characters. Cannot start with underscore.',
                $e->getMessage(),
            );
            $this->assertEquals('storage.tables.validation', $e->getStringCode());
            $this->assertEquals(400, $e->getCode());
        }

        $tableNameAnother = $tableName . '_another';
        $anotherTableId = $this->_client->{$createMethod}(
            $this->getTestBucketId(self::STAGE_IN),
            $tableNameAnother,
            new CsvFile($createFile),
            $options
        );
        try {
            $this->_client->updateTable(
                $anotherTableId,
                [
                    'displayName' => $displayName,
                ],
            );
            $this->fail('Renaming another table to existing displayname should fail');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals(
                sprintf(
                    'The table "%s" in the bucket already has the same display name "%s".',
                    $table['name'],
                    $displayName,
                ),
                $e->getMessage(),
            );
            $this->assertEquals('storage.buckets.tableAlreadyExists', $e->getStringCode());
            $this->assertEquals(400, $e->getCode());
        }
    }

    /**
     * @return array{0: string, 1: string, 2: string, 3: bool}[]
     */
    public function tableCreateData(): array
    {
        return [
            'plain csv - sync' => [
                'Languages',
                __DIR__ . '/../../_data/languages.csv',
                __DIR__ . '/../../_data/languages.csv',
                false,
            ],
            'plain csv - async' => [
                'Languages',
                __DIR__ . '/../../_data/languages.csv',
                __DIR__ . '/../../_data/languages.csv',
                true,
            ],

            'gzipped csv - sync' => [
                'Languages',
                __DIR__ . '/../../_data/languages.csv.gz',
                __DIR__ . '/../../_data/languages.csv',
                false,
            ],
            'gzipped csv - async' => [
                'Languages',
                __DIR__ . '/../../_data/languages.csv.gz',
                __DIR__ . '/../../_data/languages.csv',
                true,
            ],

            'csv with camel case columns - sync ' => [
                'Languages',
                __DIR__ . '/../../_data/languages.camel-case-columns.csv',
                __DIR__ . '/../../_data/languages.camel-case-columns.csv',
                false,
            ],
            'csv with camel case columns - async ' => [
                'Languages',
                __DIR__ . '/../../_data/languages.camel-case-columns.csv',
                __DIR__ . '/../../_data/languages.camel-case-columns.csv',
                true,
            ],

            'csv with numeric table name and numeric columns names - sync' => [
                '1',
                __DIR__ . '/../../_data/numbers.csv',
                __DIR__ . '/../../_data/numbers.csv',
                false,
            ],
            'csv with numeric table name and numeric columns names - async' => [
                '1',
                __DIR__ . '/../../_data/numbers.csv',
                __DIR__ . '/../../_data/numbers.csv',
                true,
            ],
        ];
    }

    public function testTableWithUnsupportedCharactersInNameShouldNotBeCreated(): void
    {
        try {
            $this->_client->createTable(
                $this->getTestBucketId(),
                'languages.main',
                new CsvFile(__DIR__ . '/../../_data/languages.csv'),
            );
            $this->fail('Table with dot in name should not be created');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('storage.tables.validation', $e->getStringCode());
        }
    }

    public function testTableWithEmptyColumnNamesShouldNotBeCreated(): void
    {
        try {
            $this->_client->createTableAsync(
                $this->getTestBucketId(self::STAGE_IN),
                'languages',
                new CsvFile(__DIR__ . '/../../_data/languages.invalid-column-name.csv'),
            );
            $this->fail('Table should not be created');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('storage.tables.validation.invalidColumnName', $e->getStringCode());
        }
    }

    public function testTableFromEmptyFileShouldNotBeCreated(): void
    {
        try {
            $this->_client->createTableAsync(
                $this->getTestBucketId(self::STAGE_IN),
                'languages',
                new CsvFile(__DIR__ . '/../../_data/empty.csv'),
            );
            $this->fail('Table should not be created');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('storage.tables.validation.noColumns', $e->getStringCode());
        }

        try {
            $fileId = $this->_client->uploadFile(__DIR__ . '/../../_data/empty.csv', (new \Keboola\StorageApi\Options\FileUploadOptions())
                ->setFileName('languages')
                ->setCompress(false));

            $this->_client->createTableAsyncDirect(
                $this->getTestBucketId(self::STAGE_IN),
                [
                    'name' => 'languages',
                    'dataFileId' => $fileId,
                ],
            );
            $this->fail('Table should not be created');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('storage.tables.validation.noColumns', $e->getStringCode());
        }
    }

    public function testTableCreateFromNotUploadedFileShouldThrowError(): void
    {
        $file = $this->_client->prepareFileUpload(
            (new FileUploadOptions())
                ->setFileName('missing')
                ->setFederationToken(true),
        );
        try {
            $this->_client->createTableAsyncDirect(
                $this->getTestBucketId(self::STAGE_IN),
                [
                    'name' => 'languages',
                    'dataFileId' => $file['id'],
                ],
            );
            $this->fail('Table should not be created');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('storage.fileNotUploaded', $e->getStringCode());
        }
    }

    /**
     * @dataProvider syncAsyncData
     */
    public function testTableColumnNamesSanitize(bool $async): void
    {
        $csv = new \Keboola\Csv\CsvFile(__DIR__ . '/../../_data/filtering.csv');
        if ($async === false) {
            $this->allowTestForBackendsOnly(
                [self::BACKEND_SNOWFLAKE],
                'Test sync actions only on Snowflake',
            );
        }
        $method = $async ? 'createTableAsync' : 'createTable';
        $tableId = $this->_client->{$method}(
            $this->getTestBucketId(self::STAGE_IN),
            'sanitize',
            $csv
        );

        $table = $this->_client->getTable($tableId);
        $this->assertEquals(['with_spaces', 'scrscz', 'with_underscore'], $table['columns']);
        $writeMethod = $async ? 'writeTableAsync' : 'writeTable';
        $this->_client->{$writeMethod}($tableId, new \Keboola\Csv\CsvFile(__DIR__ . '/../../_data/filtering.csv'));
    }

    /**
     * @dataProvider syncAsyncData
     */
    public function testTableWithLongColumnNamesShouldNotBeCreated(bool $async): void
    {
        if ($async === false) {
            $this->allowTestForBackendsOnly(
                [self::BACKEND_SNOWFLAKE],
                'Test sync actions only on Snowflake',
            );
        }
        try {
            $method = $async ? 'createTableAsync' : 'createTable';
            $this->_client->{$method}(
                $this->getTestBucketId(self::STAGE_IN),
                'languages',
                new CsvFile(__DIR__ . '/../../_data/long-column-names.csv')
            );
            $this->fail('Should throw exception');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('storage.tables.validation.invalidColumnName', $e->getStringCode());
        }
    }

    /**
     * @return bool[][]
     */
    public function syncAsyncData(): array
    {
        return [
            'sync' => [false],
            'async' => [true],
        ];
    }

    public function testTableCreateWithPK(): void
    {
        $tableId = $this->_client->createTableAsync(
            $this->getTestBucketId(),
            'languages',
            new CsvFile(__DIR__ . '/../../_data/languages.csv'),
            [
                'primaryKey' => 'id',
            ],
        );

        $table = $this->_client->getTable($tableId);
        $this->assertEquals(['id'], $table['primaryKey']);
    }

    /**
     * @dataProvider invalidPrimaryKeys
     * @param $backend
     */
    public function testTableCreateWithInvalidPK($primaryKey): void
    {
        try {
            $this->_client->createTableAsync(
                $this->getTestBucketId(self::STAGE_IN),
                'languages',
                new CsvFile(__DIR__ . '/../../_data/languages.csv'),
                [
                    'primaryKey' => $primaryKey,
                ],
            );
            $this->fail('exception should be thrown');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('storage.tables.validation.invalidPrimaryKeyColumns', $e->getStringCode());
        }

        try {
            $this->_client->createTableAsync(
                $this->getTestBucketId(self::STAGE_IN),
                'languages',
                new CsvFile(__DIR__ . '/../../_data/languages.csv'),
                [
                    'primaryKey' => $primaryKey,
                ],
            );
            $this->fail('exception should be thrown');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('storage.tables.validation.invalidPrimaryKeyColumns', $e->getStringCode());
        }
    }

    public function testCreateTableWithInvalidTableName(): void
    {
        $this->expectException(ClientException::class);
        $this->_client->createTableAsync(
            $this->getTestBucketId(self::STAGE_IN),
            'tableWith.Dot',
            new CsvFile(__DIR__ . '/../../_data/languages.csv'),
            [],
        );

        $this->expectException(ClientException::class);
        $this->_client->createTableAsync(
            $this->getTestBucketId(self::STAGE_IN),
            'table.WithDot',
            new CsvFile(__DIR__ . '/../../_data/languages.csv'),
            [],
        );
    }

    public function testTableCreateInvalidPkType(): void
    {
        // sync create table is deprecated and does not support JSON
        try {
            $this->_client->apiPost(
                sprintf('buckets/%s/tables', $this->getTestBucketId(self::STAGE_IN)),
                [
                    'name' => 'languages',
                    'dataString' => 'id,name',
                    'primaryKey' => ['id', 'name'],
                ],
            );
            $this->fail('table should not be created');
        } catch (ClientException $e) {
            $this->assertEquals(400, $e->getCode());
            $this->assertEquals('storage.validation.primaryKey', $e->getStringCode());
        }

        // async
        try {
            $this->_client->apiPostJson(
                sprintf('buckets/%s/tables-async', $this->getTestBucketId(self::STAGE_IN)),
                [
                    'dataFileId' => 100,
                    'name' => 'languages',
                    'dataString' => 'id,name',
                    'primaryKey' => ['id', 'name'],
                ],
            );
            $this->fail('table should not be created');
        } catch (ClientException $e) {
            $this->assertEquals(400, $e->getCode());
            $this->assertEquals('storage.validation.primaryKey', $e->getStringCode());
        }
    }

    public function testRowNumberAmbiguity(): void
    {
        $importFile = __DIR__ . '/../../_data/column-name-row-number.csv';

        // create and import data into source table
        $tableId = $this->_client->createTableAsync(
            $this->getTestBucketId(),
            'column-name-row-number',
            new CsvFile($importFile),
        );

        // this used to fail because of the column named row_number
        $this->_client->createTablePrimaryKey($tableId, ['id']);
        $this->assertNotEmpty($tableId);
    }

    /**
     * @dataProvider createTableFromSlicedFileData
     */
    public function testCreateTableFromSlicedFile($fileName): void
    {
        $uploadOptions = new \Keboola\StorageApi\Options\FileUploadOptions();
        $uploadOptions
            ->setFileName($fileName)
            ->setIsSliced(true)
            ->setIsEncrypted(false);
        $slices = [
            __DIR__ . '/../../_data/languages.no-headers.csv',
        ];
        $slicedFileId = $this->_client->uploadSlicedFile($slices, $uploadOptions);

        try {
            $this->_client->createTableAsyncDirect(
                $this->getTestBucketId(self::STAGE_IN),
                [
                    'name' => 'languages',
                    'dataFileId' => $slicedFileId,
                ],
            );
            $this->fail('Table should not be created');
        } catch (ClientException $e) {
            // it should be - cannot create a table from sliced file without header
            $this->assertEquals('storage.tables.validation.invalidColumnName', $e->getStringCode());
        }
    }

    public function invalidPrimaryKeys()
    {
        return [
            'same name but uppercase' => ['ID'],
            'different name' => ['idus'],
        ];
    }

    public function createTableFromSlicedFileData()
    {
        return [
            'same prefix as slice' => [
                'languages',
            ],
            'same prefix (due webalize) as slice' => [
                'languages_',
            ],
            'other' => [
                'other',
            ],
        ];
    }
}
