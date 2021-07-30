<?php

namespace Keboola\Test\Backend\Exasol;

use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Metadata;
use Keboola\Test\StorageApiTestCase;

class CreateTableTest extends StorageApiTestCase
{
    const TABLE_DEFINITION = [
        'name' => 'my-new-table',
        'primaryKeysNames' => ['id'],
        'columns' => [
            [
                'name' => 'id',
                'definition' => [
                    'type' => 'INT',
                ],
            ],
            [
                'name' => 'name',
                'definition' => [
                    'type' => 'NVARCHAR',
                ],
            ],
        ]
    ];

    public function setUp()
    {
        parent::setUp();

        $token = $this->_client->verifyToken();

        if (!in_array('tables-definition', $token['owner']['features'])) {
            $this->fail(sprintf('Tables definition feature is not enabled for project "%s"', $token['owner']['id']));
        }

        $this->initEmptyTestBucketsForParallelTests();
    }

    public function testCreateTableDefinition()
    {
        $bucketId = $this->getTestBucketId(self::STAGE_IN);

        $runId = $this->_client->generateRunId();
        $this->_client->setRunId($runId);

        $tableId = $this->_client->createTableDefinition($bucketId, self::TABLE_DEFINITION);

        // block until async events are processed, processing in order is not guaranteed but it should work most of time
        $this->createAndWaitForEvent((new \Keboola\StorageApi\Event())->setComponent('dummy')->setMessage('dummy'));

        //check that the job has started
        $events = $this->_client->listEvents([
            'runId' => $runId,
        ]);

        $workspaceCreatedEvent = array_pop($events);
        $this->assertSame($runId, $workspaceCreatedEvent['runId']);
        $this->assertSame('storage.tableCreated', $workspaceCreatedEvent['event']);
        $this->assertSame('storage', $workspaceCreatedEvent['component']);

        // event params validation
        $eventParams = $workspaceCreatedEvent['params'];

        $this->assertSame(['id'], $eventParams['primaryKey']);
        $this->assertSame(
            [
                'id',
                'name',
            ],
            $eventParams['columns']
        );
        $this->assertSame(
            [
                'id' => [
                    'type' => 'INT',
                    'length' => null,
                    'nullable' => true,
                ],
                'name' => [
                    'type' => 'NVARCHAR',
                    'length' => null,
                    'nullable' => true,
                ],
            ],
            $eventParams['columnsTypes']
        );
        $this->assertSame(false, $eventParams['syntheticPrimaryKeyEnabled']);

        // table properties validation
        $table = $this->_client->getTable($tableId);

        $this->assertSame('my-new-table', $table['name']);
        $this->assertSame('my-new-table', $table['displayName']);

        $this->assertSame(['id'], $table['primaryKey']);
        $this->assertSame(['id'], $table['indexedColumns']);

        $this->assertSame(false, $table['syntheticPrimaryKeyEnabled']);

        $this->assertSame(
            [
                'id',
                'name'
            ],
            $table['columns']
        );

        $this->assertCount(1, $table['metadata']);

        $metadata = reset($table['metadata']);
        $this->assertSame('storage', $metadata['provider']);
        $this->assertSame('KBC.dataTypesEnabled', $metadata['key']);
        $this->assertSame('true', $metadata['value']);
    }

    public function testCreateTableDefinitionNoPrimaryKey()
    {
        $bucketId = $this->getTestBucketId(self::STAGE_IN);

        $runId = $this->_client->generateRunId();
        $this->_client->setRunId($runId);

        $definition = self::TABLE_DEFINITION;
        // remove primaryKeysNames, try if request is validated
        unset($definition['primaryKeysNames']);

        try {
            $this->_client->createTableDefinition($bucketId, $definition);
            self::fail('Table should not be created.');
        } catch (ClientException $e) {
            self::assertSame('Invalid request', $e->getMessage());
        }

        $definition = self::TABLE_DEFINITION;
        $definition['primaryKeysNames'] = [];

        $this->_client->createTableDefinition($bucketId, $definition);

        // block until async events are processed, processing in order is not guaranteed but it should work most of time
        $this->createAndWaitForEvent((new \Keboola\StorageApi\Event())->setComponent('dummy')->setMessage('dummy'));

        //check that the job has started
        $events = $this->_client->listEvents([
            'runId' => $runId,
        ]);

        $workspaceCreatedEvent = array_pop($events);
        self::assertSame($runId, $workspaceCreatedEvent['runId']);
        self::assertSame('storage.tableCreated', $workspaceCreatedEvent['event']);
        self::assertSame('storage', $workspaceCreatedEvent['component']);

        // event params validation
        $eventParams = $workspaceCreatedEvent['params'];

        // empty PK's
        self::assertSame([], $eventParams['primaryKey']);
    }

    public function testColumnTypesInTableDefinition()
    {
        $bucketId = $this->getTestBucketId(self::STAGE_IN);

        $runId = $this->_client->generateRunId();
        $this->_client->setRunId($runId);

        $tableId = $this->_client->createTableDefinition($bucketId, self::TABLE_DEFINITION);

        // check that the new table has correct datypes in metadata
        $metadataClient = new Metadata($this->_client);
        $idColumnMetadata = $metadataClient->listColumnMetadata("{$tableId}.id");
        $nameColumnMetadata = $metadataClient->listColumnMetadata("{$tableId}.name");

        $this->assertArrayEqualsExceptKeys([
            'key' => 'KBC.datatype.type',
            'value' => 'DECIMAL',
            'provider' => 'storage',
        ], $idColumnMetadata[0], ['id', 'timestamp']);
        $this->assertArrayEqualsExceptKeys([
            'key' => 'KBC.datatype.nullable',
            'value' => '1',
            'provider' => 'storage',
        ], $idColumnMetadata[1], ['id', 'timestamp']);
        $this->assertArrayEqualsExceptKeys([
            'key' => 'KBC.datatype.basetype',
            'value' => 'NUMERIC',
            'provider' => 'storage',
        ], $idColumnMetadata[2], ['id', 'timestamp']);

        $this->assertArrayEqualsExceptKeys([
            'key' => 'KBC.datatype.type',
            'value' => 'VARCHAR',
            'provider' => 'storage',
        ], $nameColumnMetadata[0], ['id', 'timestamp']);
        $this->assertArrayEqualsExceptKeys([
            'key' => 'KBC.datatype.nullable',
            'value' => '1',
            'provider' => 'storage',
        ], $nameColumnMetadata[1], ['id', 'timestamp']);
        $this->assertArrayEqualsExceptKeys([
            'key' => 'KBC.datatype.basetype',
            'value' => 'STRING',
            'provider' => 'storage',
        ], $nameColumnMetadata[2], ['id', 'timestamp']);
        $this->assertArrayEqualsExceptKeys([
            'key' => 'KBC.datatype.length',
            'value' => '2000000',
            'provider' => 'storage',
        ], $nameColumnMetadata[3], ['id', 'timestamp']);
    }

    public function testCreateTableDefinitionWithWrongInput()
    {
        $bucketId = $this->getTestBucketId(self::STAGE_IN);

        $data = [
            'name' => 'my-new-table',
            'primaryKeysNames' => ['notInColumns'],
            'columns' => [
                [
                    'name' => 'id',
                    'definition' => [
                        'type' => 'INT',
                    ],
                ],
            ]
        ];
        try {
            $this->_client->createTableDefinition($bucketId, $data);
        } catch (ClientException $exception) {
            $this->assertEquals($exception->getStringCode(), 'validation.failed');
        }
    }
}
