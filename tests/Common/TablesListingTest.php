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

        $tableId = $this->_client->createTable(
            $this->getTestBucketId(),
            'languages',
            new CsvFile(__DIR__ . '/../_data/languages.csv')
        );
        $this->assertTrue($this->_client->tableExists($tableId));
    }

    public function testListTables(): void
    {
        $tableId = $this->_client->createTable($this->getTestBucketId(), 'languages', new CsvFile(__DIR__ . '/../_data/languages.csv'));
        $this->_client->setTableAttribute($tableId, 'test', 'something');
        $tables = $this->_client->listTables($this->getTestBucketId());

        $this->assertCount(1, $tables);

        $firstTable = reset($tables);
        $this->assertArrayHasKey('attributes', $firstTable, 'List bucket tables are returned with attributes');
        $this->assertCount(1, $firstTable['attributes']);

        $tables = $this->_client->listTables();
        $firstTable = false;
        foreach ($tables as $table) {
            if ($table['id'] != $tableId) {
                continue;
            }
            $firstTable = $table;
            break;
        }

        $this->assertArrayHasKey('attributes', $firstTable, 'List tables are returned with attributes');
        $this->assertCount(1, $firstTable['attributes']);
        $this->assertArrayHasKey('bucket', $firstTable, 'List tables are returned with attributes');
        $this->assertArrayHasKey('displayName', $firstTable['bucket']);
        $this->assertArrayHasKey('displayName', $firstTable);
        $this->assertArrayNotHasKey('columns', $firstTable);
    }

    public function testListTablesWithIncludeParam(): void
    {
        $this->_client->createTable($this->getTestBucketId(), 'languages', new CsvFile(__DIR__ . '/../_data/languages.csv'));
        $tables = $this->_client->listTables($this->getTestBucketId(), [
            'include' => '', // don't include anything
        ]);

        $firstTable = reset($tables);
        $this->assertArrayNotHasKey('attributes', $firstTable);
        $this->assertArrayNotHasKey('bucket', $firstTable);
        $this->assertArrayNotHasKey('metadata', $firstTable);
        $this->assertArrayNotHasKey('columnMetadata', $firstTable);

        $tables = $this->_client->listTables(null, [
            'include' => '', // don't include anything
        ]);

        $firstTable = reset($tables);
        $this->assertArrayNotHasKey('attributes', $firstTable);
        $this->assertArrayNotHasKey('bucket', $firstTable);
        $this->assertArrayNotHasKey('metadata', $firstTable);
        $this->assertArrayNotHasKey('columnMetadata', $firstTable);
    }

    public function testListTablesIncludeMetadata(): void
    {
        $metadataApi = new Metadata($this->_client);
        $tableId = $this->_client->createTable($this->getTestBucketId(), 'languages', new CsvFile(__DIR__ . '/../_data/languages.csv'));

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
            ]]
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
        $tableId = $this->_client->createTable($this->getTestBucketId(), 'languages', new CsvFile(__DIR__ . '/../_data/languages.csv'));
        $table2Id = $this->_client->createTable($this->getTestBucketId(), 'users', new CsvFile(__DIR__ . '/../_data/users.csv'));
        $table3Id = $this->_client->createTable($this->getTestBucketId(self::STAGE_OUT), 'dates', new CsvFile(__DIR__ . '/../_data/dates.csv'));

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

        $tables = $this->_client->listTables(null, [
            'include' => 'columns',
        ]);
        $this->assertCount(3, $tables);

        $languagesTables = $findTable($tables, $tableId);
        $this->assertEquals($tableId, $languagesTables['id']);
        $this->assertArrayHasKey('columns', $languagesTables);
        $this->assertEquals(['id', 'name'], $languagesTables['columns']);

        $usersTables = $findTable($tables, $table2Id);
        $this->assertEquals($table2Id, $usersTables['id']);
        $this->assertArrayHasKey('columns', $usersTables);
        $this->assertEquals(['id', 'name', 'city', 'sex'], $usersTables['columns']);

        $datesTables = $findTable($tables, $table3Id);
        $this->assertEquals($table3Id, $datesTables['id']);
        $this->assertArrayHasKey('columns', $usersTables);
        $this->assertEquals(['valid_from'], $datesTables['columns']);
    }

    public function testListTablesIncludeColumnMetadata(): void
    {
        $tableId = $this->_client->createTable($this->getTestBucketId(), 'languages', new CsvFile(__DIR__ . '/../_data/languages.csv'));

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
            ]]
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
        $table1Id = $this->_client->createTable($this->getTestBucketId(), 'languages1', new CsvFile(__DIR__ . '/../_data/languages.csv'));
        $table2Id = $this->_client->createTable($this->getTestBucketId(), 'languages2', new CsvFile(__DIR__ . '/../_data/languages.csv'));

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
            ]]
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

    public function testTableAttributes(): void
    {
        $tableId = $this->_client->createTable($this->getTestBucketId(), 'languages', new CsvFile(__DIR__ . '/../_data/languages.csv'));

        $table = $this->_client->getTable($tableId);
        $this->assertEmpty($table['attributes'], 'empty attributes after table create');

        // create
        $this->_client->setTableAttribute($tableId, 's', 'lala');
        $this->_client->setTableAttribute($tableId, 'other', 'hello', true);
        $table = $this->_client->getTable($tableId);

        $this->assertArrayEqualsSorted($table['attributes'], [
            [
                'name' => 's',
                'value' => 'lala',
                'protected' => false,
            ],
            [
                'name' => 'other',
                'value' => 'hello',
                'protected' => true,
            ],
        ], 'name', 'attribute set');

        // update
        $this->_client->setTableAttribute($tableId, 's', 'papa');
        $table = $this->_client->getTable($tableId);
        $this->assertArrayEqualsSorted($table['attributes'], [
            [
                'name' => 's',
                'value' => 'papa',
                'protected' => false,
            ],
            [
                'name' => 'other',
                'value' => 'hello',
                'protected' => true,
            ],
        ], 'name', 'attribute update');

        // delete
        $this->_client->deleteTableAttribute($tableId, 's');
        $table = $this->_client->getTable($tableId);
        $this->assertArrayEqualsSorted($table['attributes'], [
            [
                'name' => 'other',
                'value' => 'hello',
                'protected' => true,
            ],
        ], 'attribute delete');

        $this->_client->deleteTableAttribute($tableId, 'other');
    }

    public function testTableAttributesReplace(): void
    {
        $tableId = $this->_client->createTable($this->getTestBucketId(), 'languages', new CsvFile(__DIR__ . '/../_data/languages.csv'));
        $this->_client->setTableAttribute($tableId, 'first', 'something');

        $newAttributes = [
            [
                'name' => 'new',
                'value' => 'new',
            ],
            [
                'name' => 'second',
                'value' => 'second value',
                'protected' => true,
            ],
        ];
        $this->_client->replaceTableAttributes($tableId, $newAttributes);

        $table = $this->_client->getTable($tableId);
        $this->assertCount(count($newAttributes), $table['attributes']);

        $this->assertEquals($newAttributes[0]['name'], $table['attributes'][0]['name']);
        $this->assertEquals($newAttributes[0]['value'], $table['attributes'][0]['value']);
        $this->assertFalse($table['attributes'][0]['protected']);
    }

    public function testTableAttributesClear(): void
    {
        $tableId = $this->_client->createTable($this->getTestBucketId(), 'languages', new CsvFile(__DIR__ . '/../_data/languages.csv'));
        $this->_client->setTableAttribute($tableId, 'first', 'something');

        $this->_client->replaceTableAttributes($tableId);
        $table = $this->_client->getTable($tableId);

        $this->assertEmpty($table['attributes']);
    }

    /**
     * @param $attributes
     * @dataProvider invalidAttributes
     */
    public function testTableAttributesReplaceValidation($attributes): void
    {
        $tableId = $this->_client->createTable($this->getTestBucketId(), 'languages', new CsvFile(__DIR__ . '/../_data/languages.csv'));
        try {
            $this->_client->replaceTableAttributes($tableId, $attributes);
            $this->fail('Attributes should be invalid');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('storage.attributes.validation', $e->getStringCode());
        }
    }

    public function testTableListingIncludeAll(): void
    {
        $tableId = $this->_client->createTable($this->getTestBucketId(), 'languages', new CsvFile(__DIR__ . '/../_data/languages.csv'));

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
            ]]
        );

        $tables = $this->_client->listTables($this->getTestBucketId(), [
            'include' => 'buckets,attributes,columns,metadata,columnMetadata',
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

        // check attributes
        $this->assertArrayHasKey('attributes', $firstTable);
    }

    public function invalidAttributes()
    {
        return [
            [
                [
                    [
                        'nome' => 'ukulele',
                    ],
                    [
                        'name' => 'jehovista',
                    ],
                ],
            ],
        ];
    }


    public function testNullAtributesReplace(): void
    {
        $tableId = $this->_client->createTable($this->getTestBucketId(), 'languages', new CsvFile(__DIR__ . '/../_data/languages.csv'));
        $this->_client->replaceTableAttributes($tableId, [
            [
                'name' => 'neco',
                'value' => 'val',
            ],
            [
                'name' => 'empty',
                'value' => null,
            ],
        ]);
        $table = $this->_client->getTable($tableId);

        $expected = [
            [
                'name' => 'neco',
                'value' => 'val',
                'protected' => false,
            ],
            [
                'name' => 'empty',
                'value' => '',
                'protected' => false,
            ],
        ];
        $this->assertEquals($expected, $table['attributes']);
    }

    public function testNullAttributeValueSet(): void
    {
        $tableId = $this->_client->createTable($this->getTestBucketId(), 'languages', new CsvFile(__DIR__ . '/../_data/languages.csv'));

        $this->_client->setTableAttribute($tableId, 'test', null);
        $table = $this->_client->getTable($tableId);

        $this->assertEquals([['name' => 'test', 'value' => '', 'protected' => false]], $table['attributes']);
    }
}
