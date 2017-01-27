<?php
/**
 * Created by JetBrains PhpStorm.
 * User: martinhalamicek
 * Date: 22/05/14
 * Time: 16:38
 * To change this template use File | Settings | File Templates.
 */

namespace Keboola\Test\Backend\CommonPart1;

use Keboola\Test\StorageApiTestCase;
use Keboola\Csv\CsvFile;

class CreateTableTest extends StorageApiTestCase
{


    public function setUp()
    {
        parent::setUp();
        $this->_initEmptyTestBuckets();
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

        $expectationFileCsv = new CsvFile($expectationFile);

        $this->assertEquals($tableId, $table['id']);
        $this->assertEquals($tableName, $table['name']);
        $this->assertNotEmpty($table['created']);
        $this->assertNotEmpty($table['lastChangeDate']);
        $this->assertNotEmpty($table['lastImportDate']);
        $this->assertEquals($expectationFileCsv->getHeader(), $table['columns']);
        $this->assertEmpty($table['indexedColumns']);
        $this->assertNotEquals('0000-00-00 00:00:00', $table['created']);
        $this->assertNotEmpty($table['dataSizeBytes']);

        $this->assertLinesEqualsSorted(
            file_get_contents($expectationFile),
            $this->_client->exportTable($tableId),
            'initial data imported into table'
        );
    }

    public function tableCreateData()
    {
        return array(
            array('Languages', __DIR__ . '/../../_data/languages.csv', __DIR__ . '/../../_data/languages.csv', false),
            array('Languages', __DIR__ . '/../../_data/languages.csv', __DIR__ . '/../../_data/languages.csv', true),

            array('Languages', 'https://s3.amazonaws.com/keboola-tests/languages.csv', __DIR__ . '/../../_data/languages.csv', false),
            array('Languages', 'https://s3.amazonaws.com/keboola-tests/languages.csv', __DIR__ . '/../../_data/languages.csv', true),

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
        $this->assertEquals(array('id'), $table['indexedColumns']);
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
                array(
                    'primaryKey' => $primaryKey,
                )
            );
            $this->fail('exception should be thrown');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('storage.tables.validation.invalidPrimaryKeyColumns', $e->getStringCode());
        }
    }

    public function invalidPrimaryKeys()
    {
        return array(
            array('ID'),
            array('idus'),
        );
    }
}
