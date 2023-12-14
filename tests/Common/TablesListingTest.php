<?php
/**
 * Created by JetBrains PhpStorm.
 * User: martinhalamicek
 * Date: 22/05/14
 * Time: 16:30
 * To change this template use File | Settings | File Templates.
 */

namespace Keboola\Test\Common;

use Keboola\StorageApi\Metadata;
use Keboola\Test\StorageApiTestCase;
use Keboola\Csv\CsvFile;

class TablesListingTest extends StorageApiTestCase
{

    public function setUp(): void
    {
        parent::setUp();
        $this->_initEmptyTestBuckets();
    }

    public function testTableExists(): void
    {
        $this->assertFalse($this->_client->tableExists($this->getTestBucketId() . '.languages'));

        $tableId = $this->_client->createTableAsync(
            $this->getTestBucketId(),
            'languages',
            new CsvFile(__DIR__ . '/../_data/languages.csv'),
        );
        $this->assertTrue($this->_client->tableExists($tableId));
    }

    public function testListTables(): void
    {
        $tableId = $this->_client->createTableAsync($this->getTestBucketId(), 'languages', new CsvFile(__DIR__ . '/../_data/languages.csv'));
        $tables = $this->_client->listTables($this->getTestBucketId());

        $this->assertCount(1, $tables);

        $tables = $this->_client->listTables();
        $firstTable = false;
        foreach ($tables as $table) {
            if ($table['id'] != $tableId) {
                continue;
            }
            $firstTable = $table;
            break;
        }

        $this->assertArrayHasKey('bucket', $firstTable);
        $this->assertArrayHasKey('displayName', $firstTable['bucket']);
        $this->assertArrayHasKey('displayName', $firstTable);
        $this->assertArrayNotHasKey('columns', $firstTable);
    }

    public function testListTablesWithIncludeParam(): void
    {
        $this->_client->createTableAsync($this->getTestBucketId(), 'languages', new CsvFile(__DIR__ . '/../_data/languages.csv'));
        $tables = $this->_client->listTables($this->getTestBucketId(), [
            'include' => '', // don't include anything
        ]);

        $firstTable = reset($tables);
        $this->assertArrayNotHasKey('bucket', $firstTable);
        $this->assertArrayNotHasKey('metadata', $firstTable);
        $this->assertArrayNotHasKey('columnMetadata', $firstTable);

        $tables = $this->_client->listTables(null, [
            'include' => '', // don't include anything
        ]);

        $firstTable = reset($tables);
        $this->assertArrayNotHasKey('bucket', $firstTable);
        $this->assertArrayNotHasKey('metadata', $firstTable);
        $this->assertArrayNotHasKey('columnMetadata', $firstTable);
    }

    public function testListTablesIncludeMetadata(): void
    {
        $metadataApi = new Metadata($this->_client);
        $tableId = $this->_client->createTableAsync($this->getTestBucketId(), 'languages', new CsvFile(__DIR__ . '/../_data/languages.csv'));

        $tables = $this->_client->listTables($this->getTestBucketId(), [
            'include' => 'metadata',
        ]);

        $firstTable = reset($tables);
        $this->assertArrayHasKey('metadata', $firstTable);
        $this->assertEmpty($firstTable['metadata']);

        $metadataApi->postTableMetadata(
            $tableId,
            'keboola.sapi_client_tests',
            [[
                'key' => 'testkey',
                'value' => 'testValue',
            ],[
                'key' => 'testkey2',
                'value' => 'testValue2',
            ]],
        );

        $tables = $this->_client->listTables($this->getTestBucketId(), [
            'include' => 'metadata',
        ]);
        $this->assertCount(1, $tables);

        $firstTable = reset($tables);
        $this->assertCount(2, $firstTable['metadata']);
        $firstMeta = reset($firstTable['metadata']);
        $this->assertArrayHasKey('timestamp', $firstMeta);
        $this->assertArrayHasKey('provider', $firstMeta);
        $this->assertEquals('keboola.sapi_client_tests', $firstMeta['provider']);
        $this->assertArrayHasKey('key', $firstMeta);
        $this->assertNotEmpty($firstMeta['key']);
        $this->assertArrayHasKey('value', $firstMeta);
        $this->assertNotEmpty($firstMeta['value']);
    }

    public function testListTablesWithColumns(): void
    {
        $tableId = $this->_client->createTableAsync($this->getTestBucketId(), 'languages', new CsvFile(__DIR__ . '/../_data/languages.csv'));
        $table2Id = $this->_client->createTableAsync($this->getTestBucketId(), 'users', new CsvFile(__DIR__ . '/../_data/users.csv'));

        $tables = $this->_client->listTables($this->getTestBucketId(), [
            'include' => 'columns',
        ]);

        $this->assertCount(2, $tables);

        $findTable = function ($tables, $tableId) {
            $found = array_filter($tables, function ($table) use ($tableId) {
                return $table['id'] === $tableId;
            });
            if (count($found) === 0) {
                throw  new \Exception("Table $tableId not found");
            }
            return reset($found);
        };

        $languagesTables = $findTable($tables, $tableId);
        $this->assertEquals($tableId, $languagesTables['id']);
        $this->assertArrayHasKey('columns', $languagesTables);
        $this->assertEquals(['id', 'name'], $languagesTables['columns']);

        $usersTables = $findTable($tables, $table2Id);
        $this->assertEquals($table2Id, $usersTables['id']);
        $this->assertArrayHasKey('columns', $usersTables);
        $this->assertEquals(['id', 'name', 'city', 'sex'], $usersTables['columns']);
    }

    public function testListTablesIncludeColumnMetadata(): void
    {
        $tableId = $this->_client->createTableAsync($this->getTestBucketId(), 'languages', new CsvFile(__DIR__ . '/../_data/languages.csv'));

        $tables = $this->_client->listTables($this->getTestBucketId(), [
            'include' => 'columnMetadata,metadata',
        ]);
        $firstTable = reset($tables);
        $this->assertEquals($tableId, $firstTable['id']);
        $this->assertArrayHasKey('columnMetadata', $firstTable);
        $this->assertArrayHasKey('metadata', $firstTable);
        $this->assertEmpty($firstTable['columnMetadata']);

        // let's post some column metadata to make sure it shows up correctly
        $metadataApi = new Metadata($this->_client);
        $metadataApi->postColumnMetadata(
            $tableId . '.id',
            'keboola.sapi_client_tests',
            [[
                'key' => 'testkey',
                'value' => 'testValue',
            ],[
                'key' => 'testkey2',
                'value' => 'testValue2',
            ]],
        );
        $tables = $this->_client->listTables($this->getTestBucketId(), [
            'include' => 'metadata,columnMetadata',
        ]);

        $firstTable = reset($tables);
        $this->assertEquals($tableId, $firstTable['id']);
        $this->assertArrayHasKey('columnMetadata', $firstTable);
        $this->assertNotEmpty($firstTable['columnMetadata']);
        $this->assertCount(1, $firstTable['columnMetadata']);
        $this->assertArrayHasKey('id', $firstTable['columnMetadata']);
        $this->assertCount(2, $firstTable['columnMetadata']['id']);
        $this->assertArrayHasKey('timestamp', $firstTable['columnMetadata']['id'][0]);
        $this->assertArrayHasKey('provider', $firstTable['columnMetadata']['id'][0]);
        $this->assertEquals('keboola.sapi_client_tests', $firstTable['columnMetadata']['id'][0]['provider']);
        $this->assertArrayHasKey('key', $firstTable['columnMetadata']['id'][0]);
        $this->assertArrayHasKey('value', $firstTable['columnMetadata']['id'][0]);

        $this->assertArrayHasKey('timestamp', $firstTable['columnMetadata']['id'][1]);
        $this->assertArrayHasKey('provider', $firstTable['columnMetadata']['id'][1]);
        $this->assertEquals('keboola.sapi_client_tests', $firstTable['columnMetadata']['id'][1]['provider']);
        $this->assertArrayHasKey('key', $firstTable['columnMetadata']['id'][1]);
        $this->assertArrayHasKey('value', $firstTable['columnMetadata']['id'][1]);
    }

    public function testSomeTablesWithMetadataSomeWithout(): void
    {
        $table1Id = $this->_client->createTableAsync($this->getTestBucketId(), 'languages1', new CsvFile(__DIR__ . '/../_data/languages.csv'));
        $table2Id = $this->_client->createTableAsync($this->getTestBucketId(), 'languages2', new CsvFile(__DIR__ . '/../_data/languages.csv'));

        $metadataApi = new Metadata($this->_client);
        $metadataApi->postColumnMetadata(
            $table1Id . '.id',
            'keboola.sapi_client_tests',
            [[
                'key' => 'testkey',
                'value' => 'testValue',
            ],[
                'key' => 'testkey2',
                'value' => 'testValue2',
            ]],
        );

        $tables = $this->_client->listTables($this->getTestBucketId(), [
            'include' => 'columnMetadata,metadata',
        ]);

        $this->assertCount(2, $tables);
        foreach ($tables as $table) {
            $this->assertArrayHasKey('columnMetadata', $table);
            $this->assertArrayHasKey('metadata', $table);
            $this->assertEmpty($table['metadata']);
            if ($table['name'] === 'languages1') {
                $this->assertEquals($table1Id, $table['id']);
                $this->assertCount(1, $table['columnMetadata']);
                $this->assertArrayHasKey('id', $table['columnMetadata']);
                $this->assertCount(2, $table['columnMetadata']['id']);
            } else {
                $this->assertEquals($table2Id, $table['id']);
                $this->assertEmpty($table['columnMetadata']);
            }
        }
    }

    public function testTableListingIncludeAll(): void
    {
        $tableId = $this->_client->createTableAsync($this->getTestBucketId(), 'languages', new CsvFile(__DIR__ . '/../_data/languages.csv'));

        $metadataApi = new Metadata($this->_client);
        $metadataApi->postColumnMetadata(
            $tableId . '.id',
            'keboola.sapi_client_tests',
            [[
                'key' => 'testkey',
                'value' => 'testValue',
            ],[
                'key' => 'testkey2',
                'value' => 'testValue2',
            ]],
        );

        $tables = $this->_client->listTables($this->getTestBucketId(), [
            'include' => 'buckets,columns,metadata,columnMetadata',
        ]);

        // check the columns
        $firstTable = reset($tables);
        $this->assertEquals($tableId, $firstTable['id']);
        $this->assertArrayHasKey('columns', $firstTable);
        $this->assertEquals(['id', 'name'], $firstTable['columns']);

        // check the bucket
        $this->assertArrayHasKey('bucket', $firstTable);
        $this->assertEquals($this->getTestBucketId(), $firstTable['bucket']['id']);

        // check metadata
        $this->assertArrayHasKey('columnMetadata', $firstTable);
        $this->assertArrayHasKey('metadata', $firstTable);
        $this->assertEmpty($firstTable['metadata']);
        $this->assertEquals($tableId, $firstTable['id']);
        $this->assertCount(1, $firstTable['columnMetadata']);
        $this->assertArrayHasKey('id', $firstTable['columnMetadata']);
        $this->assertCount(2, $firstTable['columnMetadata']['id']);
    }
}
