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

    public function testSyntheticPrimaryKey(): void
    {
        $testBucketName = $this->getTestBucketName($this->getTestBucketId());
        $testBucketStage = self::STAGE_IN;
        $testBucketId = $testBucketStage . '.c-' . $testBucketName;
        $tableName = 'testSynthPk';
        $testTableId = $testBucketId . '.' . $tableName;

        $this->dropBucketIfExists($this->_client, $testBucketId);
        $testBucketId = $this->_client->createBucket($testBucketName, self::STAGE_IN);
        $this->_client->createTable(
            $testBucketId,
            $tableName,
            new CsvFile(__DIR__ . '/../../_data/languages.csv')
        );

        // test spk part of detail
        $table = $this->_client->getTable($testBucketId . '.' . $tableName);
        $this->assertFalse($table['syntheticPrimaryKeyEnabled']);

        try {
            $tableId = $this->_client->createTable(
                $testBucketId,
                'myTable',
                new CsvFile(__DIR__ . '/../../_data/languages.csv'),
                ['syntheticPrimaryKeyEnabled' => 1, 'primaryKey' => 'id,name']
            );
            $this->fail('Should have thrown');
        } catch (ClientException $e) {
            $this->assertSame(
                'Synthetic primary key cannot be enabled in sync call, use async call to create the table',
                $e->getMessage()
            );
        }

        try {
            $tableId = $this->_client->createTableAsync(
                $testBucketId,
                'myTable',
                new CsvFile(__DIR__ . '/../../_data/languages.csv'),
                ['syntheticPrimaryKeyEnabled' => 1]
            );
            $this->fail('Should have thrown');
        } catch (ClientException $e) {
            $this->assertSame('Cannot set synthetic primary key if primary key is empty', $e->getMessage());
        }

        $this->markTestSkipped('Following needs SPK enabled on table');

        try {
            $tableId = $this->_client->createTableAsync(
                $testBucketId,
                'myTable',
                new CsvFile(__DIR__ . '/../../_data/languages.csv'),
                ['syntheticPrimaryKeyEnabled' => 1, 'primaryKey' => 'id,name']
            );
            $this->fail('Should have thrown');
        } catch (ClientException $e) {
            $this->assertSame('Synthetic primary key is not supported outside Snowflake and ABS', $e->getMessage());
        }

        try {
            $this->_client->createTablePrimaryKey($testTableId, ['id']);
            $this->fail('Should have thrown');
        } catch (ClientException $e) {
            $this->assertSame(
                sprintf('Cannot alter primary key of table "%s.testSynthPk" ', $testBucketId) .
                'with synthetic primary key enabled',
                $e->getMessage()
            );
        }
        try {
            $this->_client->removeTablePrimaryKey($testTableId, ['id']);
            $this->fail('Should have thrown');
        } catch (ClientException $e) {
            $this->assertSame(
                sprintf('Cannot alter primary key of table "%s.testSynthPk" ', $testBucketId) .
                'with synthetic primary key enabled',
                $e->getMessage()
            );
        }
    }

    public function testLoadWithInvalidCSVColumns(): void
    {
        if ($this->getDefaultBackend($this->_client) === self::BACKEND_SYNAPSE) {
            $this->markTestSkipped('Synapse does not fail on invalid data');
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
     * @param $createFile
     */
    public function testTableCreate($tableName, $createFile, $expectationFile, $async, $options = []): void
    {
        if ($tableName === '1' && $this->getDefaultBackend($this->_client) === self::BACKEND_BIGQUERY) {
            $this->markTestSkipped('Bigquery doesn\'t support number as column name');
        }
        $createMethod = $async ? 'createTableAsync' : 'createTable';
        $tableId = $this->_client->{$createMethod}(
            $this->getTestBucketId(self::STAGE_IN),
            $tableName,
            new CsvFile($createFile),
            $options
        );
        $table = $this->_client->getTable($tableId);

        $this->assertArrayHasKey('displayName', $table['bucket']);

        $expectationFileCsv = new CsvFile($expectationFile);

        $this->assertEquals($tableId, $table['id']);
        $this->assertEquals($tableName, $table['name']);
        $this->assertEquals($tableName, $table['displayName'], 'display name is same as name');
        $this->assertNotEmpty($table['created']);
        $this->assertNotEmpty($table['lastChangeDate']);
        $this->assertNotEmpty($table['lastImportDate']);
        $this->assertEquals($expectationFileCsv->getHeader(), $table['columns']);
        $this->assertEmpty($table['indexedColumns']);
        $this->assertNotEquals('0000-00-00 00:00:00', $table['created']);
        $this->assertNotEmpty($table['dataSizeBytes']);

        if ($this->getDefaultBackend($this->_client) !== self::BACKEND_BIGQUERY) {
            $this->assertLinesEqualsSorted(
                file_get_contents($expectationFile),
                $this->_client->getTableDataPreview($tableId),
                'initial data imported into table'
            );
        }
        $displayName = 'Romanov-display-name';
        $tableId = $this->_client->updateTable(
            $tableId,
            [
                'displayName' => $displayName,
            ]
        );
        assert($tableId !== null);

        $table = $this->_client->getTable($tableId);

        $this->assertEquals($displayName, $table['displayName']);

        // rename table to same name it already has should succeed
        $this->_client->updateTable(
            $tableId,
            [
                'displayName' => $displayName,
            ]
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
                    $displayName
                ),
                $e->getMessage()
            );
            $this->assertEquals('storage.buckets.tableAlreadyExists', $e->getStringCode());
        }

        try {
            $this->_client->updateTable(
                $tableId,
                [
                    'displayName' => '_wrong-display-name',
                ]
            );
            $this->fail('Should fail');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals(
                'Invalid data - displayName: Cannot start with underscore.',
                $e->getMessage()
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
                ]
            );
            $this->fail('Renaming another table to existing displayname should fail');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals(
                sprintf(
                    'The table "%s" in the bucket already has the same display name "%s".',
                    $table['name'],
                    $displayName
                ),
                $e->getMessage()
            );
            $this->assertEquals('storage.buckets.tableAlreadyExists', $e->getStringCode());
            $this->assertEquals(400, $e->getCode());
        }
    }

    public function tableCreateData()
    {
        return [
            ['Languages', __DIR__ . '/../../_data/languages.csv', __DIR__ . '/../../_data/languages.csv', false],
            ['Languages', __DIR__ . '/../../_data/languages.csv', __DIR__ . '/../../_data/languages.csv', true],

            ['Languages', __DIR__ . '/../../_data/languages.csv.gz', __DIR__ . '/../../_data/languages.csv', false],
            ['Languages', __DIR__ . '/../../_data/languages.csv.gz', __DIR__ . '/../../_data/languages.csv', true],

            ['Languages', __DIR__ . '/../../_data/languages.camel-case-columns.csv', __DIR__ . '/../../_data/languages.camel-case-columns.csv', false],
            ['Languages', __DIR__ . '/../../_data/languages.camel-case-columns.csv', __DIR__ . '/../../_data/languages.camel-case-columns.csv', true],

            // only numeric table and column names
            ['1', __DIR__ . '/../../_data/numbers.csv', __DIR__ . '/../../_data/numbers.csv', false],
            ['1', __DIR__ . '/../../_data/numbers.csv', __DIR__ . '/../../_data/numbers.csv', true],
        ];
    }

    public function testTableWithUnsupportedCharactersInNameShouldNotBeCreated(): void
    {
        try {
            $tableId = $this->_client->createTable(
                $this->getTestBucketId(),
                'languages.main',
                new CsvFile(__DIR__ . '/../../_data/languages.csv')
            );
            $this->fail('Table with dot in name should not be created');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('storage.tables.validation', $e->getStringCode());
        }
    }

    public function testTableWithEmptyColumnNamesShouldNotBeCreated(): void
    {
        try {
            $this->_client->createTable(
                $this->getTestBucketId(self::STAGE_IN),
                'languages',
                new CsvFile(__DIR__ . '/../../_data/languages.invalid-column-name.csv')
            );
            $this->fail('Table should not be created');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('storage.tables.validation.invalidColumnName', $e->getStringCode());
        }
    }

    public function testTableFromEmptyFileShouldNotBeCreated(): void
    {
        try {
            $this->_client->createTable(
                $this->getTestBucketId(self::STAGE_IN),
                'languages',
                new CsvFile(__DIR__ . '/../../_data/empty.csv')
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
                ]
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
                ->setFederationToken(true)
        );
        try {
            $this->_client->createTableAsyncDirect(
                $this->getTestBucketId(self::STAGE_IN),
                [
                    'name' => 'languages',
                    'dataFileId' => $file['id'],
                ]
            );
            $this->fail('Table should not be created');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('storage.fileNotUploaded', $e->getStringCode());
        }
    }

    /**
     * @param $async
     * @dataProvider syncAsyncData
     */
    public function testTableColumnNamesSanitize($async): void
    {
        $csv = new \Keboola\Csv\CsvFile(__DIR__ . '/../../_data/filtering.csv');

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
     * @param $backend
     * @param $async
     * @dataProvider syncAsyncData
     */
    public function testTableWithLongColumnNamesShouldNotBeCreated($async): void
    {
        try {
            $method = $async ? 'createTableAsync' : 'createTable';
            $this->_client->{$method}(
                $this->getTestBucketId(self::STAGE_IN),
                'languages',
                new CsvFile(__DIR__ . '/../../_data/long-column-names.csv')
            );
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('storage.tables.validation.invalidColumnName', $e->getStringCode());
        }
    }

    public function syncAsyncData()
    {
        return [
            [false],
            [true],
        ];
    }

    public function testTableCreateWithPK(): void
    {
        if ($this->getDefaultBackend($this->_client) === self::BACKEND_TERADATA
        || $this->getDefaultBackend($this->_client) === self::BACKEND_BIGQUERY
        ) {
            $this->markTestSkipped('deduplication not supported for Teradata');
        }
        $tableId = $this->_client->createTable(
            $this->getTestBucketId(),
            'languages',
            new CsvFile(__DIR__ . '/../../_data/languages.csv'),
            [
                'primaryKey' => 'id',
            ]
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
            $this->_client->createTable(
                $this->getTestBucketId(self::STAGE_IN),
                'languages',
                new CsvFile(__DIR__ . '/../../_data/languages.csv'),
                [
                    'primaryKey' => $primaryKey,
                ]
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
                ]
            );
            $this->fail('exception should be thrown');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('storage.tables.validation.invalidPrimaryKeyColumns', $e->getStringCode());
        }
    }

    public function testCreateTableWithInvalidTableName(): void
    {
        $this->expectException(ClientException::class);
        $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN),
            'tableWith.Dot',
            new CsvFile(__DIR__ . '/../../_data/languages.csv'),
            []
        );

        $this->expectException(ClientException::class);
        $this->_client->createTableAsync(
            $this->getTestBucketId(self::STAGE_IN),
            'table.WithDot',
            new CsvFile(__DIR__ . '/../../_data/languages.csv'),
            []
        );
    }

    public function testTableCreateInvalidPkType(): void
    {
        // sync
        try {
            $this->_client->apiPost(
                sprintf('buckets/%s/tables', $this->getTestBucketId(self::STAGE_IN)),
                [
                    'name' => 'languages',
                    'dataString' => 'id,name',
                    'primaryKey' => ['id', 'name'],
                ]
            );
            $this->fail('table should not be created');
        } catch (ClientException $e) {
            $this->assertEquals(400, $e->getCode());
            $this->assertEquals('storage.validation.primaryKey', $e->getStringCode());
        }

        // async
        try {
            $this->_client->apiPost(
                sprintf('buckets/%s/tables-async', $this->getTestBucketId(self::STAGE_IN)),
                [
                    'dataFileId' => 100,
                    'name' => 'languages',
                    'dataString' => 'id,name',
                    'primaryKey' => ['id', 'name'],
                ]
            );
            $this->fail('table should not be created');
        } catch (ClientException $e) {
            $this->assertEquals(400, $e->getCode());
            $this->assertEquals('storage.validation.primaryKey', $e->getStringCode());
        }
    }

    public function testRowNumberAmbiguity(): void
    {
        if ($this->getDefaultBackend($this->_client) === self::BACKEND_TERADATA
        || $this->getDefaultBackend($this->_client) === self::BACKEND_BIGQUERY
        ) {
            $this->markTestSkipped('createTablePrimaryKey not supported for Teradata');
        }
        $importFile = __DIR__ . '/../../_data/column-name-row-number.csv';

        // create and import data into source table
        $tableId = $this->_client->createTable(
            $this->getTestBucketId(),
            'column-name-row-number',
            new CsvFile($importFile)
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
                ]
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
            ['ID'],
            ['idus'],
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
