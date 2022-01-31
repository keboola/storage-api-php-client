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


    public function setUp()
    {
        parent::setUp();
        $this->initEmptyTestBucketsForParallelTests();
    }

    public function testSyntheticPrimaryKey()
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

    /**
     * @dataProvider tableCreateData
     * @param $createFile
     */
    public function testTableCreate($tableName, $createFile, $expectationFile, $async, $options = array())
    {
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

        $this->assertLinesEqualsSorted(
            file_get_contents($expectationFile),
            $this->_client->getTableDataPreview($tableId),
            'initial data imported into table'
        );

        $displayName = 'Romanov-display-name';
        $tableId = $this->_client->updateTable(
            $tableId,
            [
                'displayName' => $displayName,
            ]
        );

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
        return array(
            array('Languages', __DIR__ . '/../../_data/languages.csv', __DIR__ . '/../../_data/languages.csv', false),
            array('Languages', __DIR__ . '/../../_data/languages.csv', __DIR__ . '/../../_data/languages.csv', true),

            array('Languages', __DIR__ . '/../../_data/languages.csv.gz', __DIR__ . '/../../_data/languages.csv', false),
            array('Languages', __DIR__ . '/../../_data/languages.csv.gz', __DIR__ . '/../../_data/languages.csv', true),

            array('Languages', __DIR__ . '/../../_data/languages.camel-case-columns.csv', __DIR__ . '/../../_data/languages.camel-case-columns.csv', false),
            array('Languages', __DIR__ . '/../../_data/languages.camel-case-columns.csv', __DIR__ . '/../../_data/languages.camel-case-columns.csv', true),

            // only numeric table and column names
            array('1', __DIR__ . '/../../_data/numbers.csv', __DIR__ . '/../../_data/numbers.csv', false),
            array('1', __DIR__ . '/../../_data/numbers.csv', __DIR__ . '/../../_data/numbers.csv', true),
        );
    }

    public function testTableWithUnsupportedCharactersInNameShouldNotBeCreated()
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

    public function testTableWithEmptyColumnNamesShouldNotBeCreated()
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

    public function testTableFromEmptyFileShouldNotBeCreated()
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
                    'dataFileId' => $fileId
                ]
            );
            $this->fail('Table should not be created');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('storage.tables.validation.noColumns', $e->getStringCode());
        }
    }

    public function testTableCreateFromNotUploadedFileShouldThrowError()
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
    public function testTableColumnNamesSanitize($async)
    {
        $csv = new \Keboola\Csv\CsvFile(__DIR__ . '/../../_data/filtering.csv');

        $method = $async ? 'createTableAsync' : 'createTable';
        $tableId = $this->_client->{$method}(
            $this->getTestBucketId(self::STAGE_IN),
            'sanitize',
            $csv
        );

        $table = $this->_client->getTable($tableId);
        $this->assertEquals(array('with_spaces', 'scrscz', 'with_underscore'), $table['columns']);
        $writeMethod = $async ? 'writeTableAsync' : 'writeTable';
        $this->_client->{$writeMethod}($tableId, new \Keboola\Csv\CsvFile(__DIR__ . '/../../_data/filtering.csv'));
    }

    /**
     * @param $backend
     * @param $async
     * @dataProvider syncAsyncData
     */
    public function testTableWithLongColumnNamesShouldNotBeCreated($async)
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
        return array(
            array(false),
            array(true),
        );
    }

    public function testTableCreateWithPK()
    {
        $tableId = $this->_client->createTable(
            $this->getTestBucketId(),
            'languages',
            new CsvFile(__DIR__ . '/../../_data/languages.csv'),
            array(
                'primaryKey' => 'id',
            )
        );

        $table = $this->_client->getTable($tableId);
        $this->assertEquals(array('id'), $table['primaryKey']);
    }

    /**
     * @dataProvider invalidPrimaryKeys
     * @param $backend
     */
    public function testTableCreateWithInvalidPK($primaryKey)
    {
        try {
            $this->_client->createTable(
                $this->getTestBucketId(self::STAGE_IN),
                'languages',
                new CsvFile(__DIR__ . '/../../_data/languages.csv'),
                array(
                    'primaryKey' => $primaryKey,
                )
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

    public function testCreateTableWithInvalidTableName()
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

    public function testTableCreateInvalidPkType()
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

    public function testRowNumberAmbiguity()
    {
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
    public function testCreateTableFromSlicedFile($fileName)
    {
        $uploadOptions = new \Keboola\StorageApi\Options\FileUploadOptions();
        $uploadOptions
            ->setFileName($fileName)
            ->setIsSliced(true)
            ->setIsEncrypted(false);
        $slices = [
            __DIR__ . '/../../_data/languages.no-headers.csv'
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
        return array(
            array('ID'),
            array('idus'),
        );
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
