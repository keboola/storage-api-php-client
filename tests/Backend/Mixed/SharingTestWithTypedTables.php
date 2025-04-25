<?php

namespace Keboola\Test\Backend\Mixed;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Metadata;
use Keboola\Test\Backend\WorkspaceConnectionTrait;

class SharingTestWithTypedTables extends StorageApiSharingTestCase
{
    use WorkspaceConnectionTrait;

    public function testLinkedBucketTypedTable(): void
    {
        $this->initTestBuckets(self::BACKEND_SNOWFLAKE);
        $bucketId = reset($this->_bucketIds);

        $tableDefinition = [
            'name' => 'first',
            'primaryKeysNames' => ['id'],
            'columns' => [
                [
                    'name' => 'id',
                    'definition' => [
                        'type' => 'VARCHAR',
                        'nullable' => false,
                    ],
                ],
                [
                    'name' => 'name',
                    'definition' => [
                        'type' => 'VARCHAR',
                        'length' => '100',
                    ],
                ],
            ],
        ];
        $tableId = $this->_client->createTableDefinition($bucketId, $tableDefinition);

        $displayName = 'display-name-first';
        $this->_client->updateTable($tableId, ['displayName' => $displayName]);

        $table2Name = 'languages-out';
        $tableDefinition['name'] = $table2Name;
        $table2Id = $this->_client->createTableDefinition($bucketId, $tableDefinition);

        $metadataApi = new Metadata($this->_client);
        $testMetadata = [
            [
                'key' => 'test_metadata_key1',
                'value' => 'testval',
            ],
            [
                'key' => 'test_metadata_key2',
                'value' => 'testval',
            ],
        ];

        $columnId = $table2Id . '.id';
        $expectedMetadata = $metadataApi->postColumnMetadata($columnId, self::TEST_METADATA_PROVIDER, $testMetadata);

        $aliasTableId = $this->_client->createAliasTable(
            $bucketId,
            $table2Id,
            'languages-alias',
        );
        // test that table definition is visible in source table of alias
        $table2 = $this->_client->getTable($table2Id);
        $alias = $this->_client->getTable($aliasTableId);
        $this->assertArrayHasKey('definition', $table2);
        $this->assertArrayHasKey('definition', $alias['sourceTable']);
        $this->assertSame($table2['definition'], $alias['sourceTable']['definition']);

        $this->_client->shareOrganizationBucket($bucketId);

        // link
        $response = $this->_client2->listSharedBuckets();
        $this->assertCount(1, $response);

        $sharedBucket = reset($response);

        $linkedBucketId = $this->_client2->linkBucket(
            'linked-' . time(),
            'in',
            $sharedBucket['project']['id'],
            $sharedBucket['id'],
            null,
            true,
        );

        // validate bucket
        $bucket = $this->_client->getBucket($bucketId);
        $linkedBucket = $this->_client2->getBucket($linkedBucketId);

        $this->assertEquals($linkedBucketId, $linkedBucket['id']);
        $this->assertEquals('in', $linkedBucket['stage']);
        $this->assertEquals($bucket['backend'], $linkedBucket['backend']);
        $this->assertEquals($bucket['description'], $linkedBucket['description']);

        $this->assertTablesMetadata($bucketId, $linkedBucketId, true);

        // test that table definition is visible in source table of lined table
        $table2Linked = $this->_client2->getTable($linkedBucketId . '.' . $table2Name);
        $this->assertArrayHasKey('definition', $table2);
        $this->assertArrayHasKey('definition', $table2Linked['sourceTable']);
        $this->assertSame($table2['definition'], $table2Linked['sourceTable']['definition']);

        // new import
        $this->_client->writeTableAsync(
            $tableId,
            new CsvFile(__DIR__ . '/../../_data/pk.simple.increment.csv'),
            [
                'primaryKey' => 'id',
                'incremental' => true,
            ],
        );

        $this->assertTablesMetadata($bucketId, $linkedBucketId, true);

        // remove primary key
        $this->_client->removeTablePrimaryKey($tableId);
        $this->assertTablesMetadata($bucketId, $linkedBucketId, true);

        // add primary key
        $this->_client->createTablePrimaryKey($tableId, ['id', 'name']);
        $this->assertTablesMetadata($bucketId, $linkedBucketId);

        // add column
        $this->_client->addTableColumn($tableId, 'fake', null, 'STRING');
        $this->assertTablesMetadata($bucketId, $linkedBucketId, true);

        // delete rows
        $this->_client->deleteTableRows($tableId, [
            'whereColumn' => 'id',
            'whereValues' => ['new'],
        ]);
        $this->assertTablesMetadata($bucketId, $linkedBucketId, true);

        // aditional table
        $tableDefinition['name'] = 'second';
        $this->_client->createTableDefinition($bucketId, $tableDefinition);
        $aliasId = $this->_client->createAliasTable(
            $bucketId,
            $table2Id,
            'languages-alias-2',
        );
        $this->assertTablesMetadata($bucketId, $linkedBucketId);
        $aliasTable = $this->_client->getTable($aliasId);
        $this->assertSame($expectedMetadata, $aliasTable['sourceTable']['columnMetadata']['id']);
    }
}
