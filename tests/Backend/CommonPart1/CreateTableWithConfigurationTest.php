<?php

namespace Keboola\Test\Backend\CommonPart1;

use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\Options\Components\Configuration;
use Keboola\StorageApi\Options\TableWithConfigurationOptions;
use Keboola\Test\Backend\TableWithConfigurationUtils;
use Keboola\Test\ClientProvider\ClientProvider;
use Keboola\Test\StorageApiTestCase;
use Keboola\Test\Utils\EventsQueryBuilder;
use Keboola\Test\Utils\EventTesterUtils;

class CreateTableWithConfigurationTest extends StorageApiTestCase
{
    use EventTesterUtils;
    use TableWithConfigurationUtils;

    private ClientProvider $clientProvider;
    private Client $client;
    private Components $componentsClient;

    public function setUp(): void
    {
        parent::setUp();

        // check feature
        $token = $this->_client->verifyToken();
        if (!in_array('tables-with-configuration', $token['owner']['features'])) {
            $this->markTestSkipped(sprintf('Creating tables from configurations feature is not enabled for project "%s"', $token['owner']['id']));
        }

        if ($token['owner']['defaultBackend'] !== self::BACKEND_SYNAPSE) {
            self::markTestSkipped(sprintf(
                'Backend "%s" is not supported tables with configuration',
                $token['owner']['defaultBackend'],
            ));
        }

        // init buckets
        $this->initEmptyTestBucketsForParallelTests();

        $this->clientProvider = new ClientProvider($this);
        $this->client = $this->clientProvider->createClientForCurrentTest();

        $this->assertComponentExists();

        $this->configId = sha1($this->generateDescriptionForTestObject());
        $this->dropTableAndConfiguration($this->configId);

        $this->initEvents($this->client);
    }

    public function testTableCreate(): void
    {
        // create test configuration
        $configuration = (new Configuration())
            ->setComponentId(self::CUSTOM_QUERY_MANAGER_COMPONENT_ID)
            ->setConfigurationId($this->configId)
            ->setName($this->configId)
            ->setConfiguration([
                'parameters' => [
                    'migrations' => [
                        [
                            'sql' => 'CREATE TABLE {{ id(bucketName) }}.{{ id(tableName) }} ("id" integer, "name" varchar(100), [_timestamp] DATETIME2)',
                            'description' => 'first ever',
                        ],
                    ],
                ],
            ]);
        $this->componentsClient->addConfiguration($configuration);

        // create table from config
        $tableName = 'custom-table-1';
        $configurationOptions = (new TableWithConfigurationOptions($tableName, $this->configId));
        $tableId = $this->_client->createTableWithConfiguration(
            $this->getTestBucketId(),
            $configurationOptions,
        );

        $table = $this->_client->getTable($tableId);

        $this->assertEquals($tableId, $table['id']);
        $this->assertEquals($tableName, $table['name']);
        $this->assertEquals($tableName, $table['displayName'], 'display name is same as name');

        $this->assertArrayHasKey('configuration', $table);
        $this->assertArrayHasKey('branchId', $table['configuration']);
        $this->assertArrayHasKey('componentId', $table['configuration']);
        $this->assertSame('keboola.app-custom-query-manager', $table['configuration']['componentId']);
        $this->assertArrayHasKey('migrationIndex', $table['configuration']);
        $this->assertSame(0, $table['configuration']['migrationIndex']);
        $this->assertArrayHasKey('configurationId', $table['configuration']);
        $this->assertSame($this->configId, $table['configuration']['configurationId']);

        $this->assertTrue($table['isTyped']);
        $this->assertNotEmpty($table['created']);
        $this->assertEquals(['id', 'name'], $table['columns']);

        $this->assertTableColumnMetadata([
            'id' => [
                'KBC.datatype.type' => 'INT',
                'KBC.datatype.nullable' => '1',
                'KBC.datatype.basetype' => 'INTEGER',
            ],
            'name' => [
                'KBC.datatype.type' => 'VARCHAR',
                'KBC.datatype.nullable' => '1',
                'KBC.datatype.basetype' => 'STRING',
                'KBC.datatype.length' => '100',
            ],
        ], $table);

        $assertCallback = function ($events) {
            $this->assertCount(1, $events);
        };

        $query = new EventsQueryBuilder();
        $query->setEvent('storage.tableWithConfigurationMigrated')
            ->setTokenId($this->tokenId)
            ->setObjectId($tableId);
        $this->assertEventWithRetries($this->client, $assertCallback, $query);

        $assertCallback = function ($events) {
            $this->assertCount(1, $events);
            $event = reset($events);
            $this->assertArrayHasKey('params', $event);
            $this->assertArrayHasKey('columns', $event['params']);
            $this->assertEqualsIgnoringCase(['id', 'name'], $event['params']['columns']);
        };

        $query = new EventsQueryBuilder();
        $query->setEvent('storage.tableCreated')
            ->setTokenId($this->tokenId)
            ->setObjectId($tableId);
        $this->assertEventWithRetries($this->client, $assertCallback, $query);
    }

    public function testTableCreateWithMeaningFullQueryAsSecond(): void
    {
        // create test configuration
        $configuration = (new Configuration())
            ->setComponentId(self::CUSTOM_QUERY_MANAGER_COMPONENT_ID)
            ->setConfigurationId($this->configId)
            ->setName($this->configId)
            ->setConfiguration([
                'parameters' => [
                    'migrations' => [
                        [
                            'sql' => 'SELECT 1',
                            'description' => 'first ever',
                        ],
                        [
                            'sql' => 'CREATE TABLE {{ id(bucketName) }}.{{ id(tableName) }} (id integer, name varchar(100), [_timestamp] DATETIME2)',
                            'description' => 'second query',
                        ],
                    ],
                ],
            ]);
        $this->componentsClient->addConfiguration($configuration);

        // create table from config
        $tableName = 'custom-table-1';
        $configurationOptions = (new TableWithConfigurationOptions($tableName, $this->configId));
        $tableId = $this->_client->createTableWithConfiguration(
            $this->getTestBucketId(),
            $configurationOptions,
        );

        $table = $this->_client->getTable($tableId);

        $this->assertEquals($tableId, $table['id']);
        $this->assertEquals($tableName, $table['name']);
        $this->assertEquals($tableName, $table['displayName'], 'display name is same as name');
        $this->assertNotEmpty($table['created']);

        $this->assertTableColumnMetadata([
            'id' => [
                'KBC.datatype.type' => 'INT',
                'KBC.datatype.nullable' => '1',
                'KBC.datatype.basetype' => 'INTEGER',
            ],
            'name' => [
                'KBC.datatype.type' => 'VARCHAR',
                'KBC.datatype.nullable' => '1',
                'KBC.datatype.basetype' => 'STRING',
                'KBC.datatype.length' => '100',
            ],
        ], $table);

        // check events
        $assertCallback = function ($events) {
            $this->assertCount(2, $events);
            $event = end($events);
            $this->assertArrayHasKey('params', $event);
            $this->assertArrayHasKey('executedQuery', $event['params']);
            $this->assertSame('SELECT 1', $event['params']['executedQuery']);
            $event = prev($events);
            $this->assertArrayHasKey('params', $event);
            $this->assertArrayHasKey('executedQuery', $event['params']);
            $this->assertStringContainsString('CREATE TABLE', $event['params']['executedQuery']);
        };
        $query = new EventsQueryBuilder();
        $query->setEvent('storage.tableWithConfigurationMigrated')
            ->setTokenId($this->tokenId)
            ->setObjectId($tableId);
        $this->assertEventWithRetries($this->client, $assertCallback, $query);

        $assertCallback = function ($events) {
            $this->assertCount(1, $events);
        };

        $query = new EventsQueryBuilder();
        $query->setEvent('storage.tableCreated')
            ->setTokenId($this->tokenId)
            ->setObjectId($tableId);
        $this->assertEventWithRetries($this->client, $assertCallback, $query);
    }

    public function testTableCreateWithToothLessQuery(): void
    {
        // create test configuration
        $configuration = (new Configuration())
            ->setComponentId(self::CUSTOM_QUERY_MANAGER_COMPONENT_ID)
            ->setConfigurationId($this->configId)
            ->setName($this->configId)
            ->setConfiguration([
                'parameters' => [
                    'migrations' => [
                        [
                            'sql' => 'SELECT 1',
                        ],
                    ],
                ],
            ]);
        $this->componentsClient->addConfiguration($configuration);

        // create table from config
        $tableName = 'custom-table-1';
        $configurationOptions = (new TableWithConfigurationOptions($tableName, $this->configId));
        $this->expectExceptionMessage('Configuration did not create any table');
        $this->_client->createTableWithConfiguration(
            $this->getTestBucketId(),
            $configurationOptions,
        );

        $assertCallback = function ($events) {
            $this->assertCount(1, $events);
        };
        $objectId = $this->getTestBucketId() . '.custom-table-1';

        $query = new EventsQueryBuilder();
        $query->setEvent('storage.tableWithConfigurationMigrated')
            ->setTokenId($this->tokenId)
            ->setObjectId($objectId);
        $this->assertEventWithRetries($this->client, $assertCallback, $query);

        $this->expectExceptionMessage('There were no events');
        $this->listEventsFilteredByName($this->client, 'storage.tableCreated', null, 10);
    }

    public function testTableWithUnsupportedCharactersInNameShouldNotBeCreated(): void
    {
        // create test configuration
        $configuration = (new Configuration())
            ->setComponentId(self::CUSTOM_QUERY_MANAGER_COMPONENT_ID)
            ->setConfigurationId($this->configId)
            ->setName($this->configId)
            ->setConfiguration(['value' => 1]);
        $this->componentsClient->addConfiguration($configuration);

        try {
            // create table from config
            $tableName = 'custom.table.1';
            $configurationOptions = (new TableWithConfigurationOptions($tableName, $this->configId));
            $this->_client->createTableWithConfiguration(
                $this->getTestBucketId(),
                $configurationOptions,
            );
            $this->fail('Table with dot in name should not be created');
        } catch (ClientException $e) {
            $this->assertEquals('validation.failed', $e->getStringCode());
        }
    }

    public function testTableWithInvalidConfigurationIdShouldNotBeCreated(): void
    {
        try {
            // create table from config
            $tableName = 'custom-table-1';
            $configurationOptions = (new TableWithConfigurationOptions($tableName, 'doesNotExist'));
            $this->_client->createTableWithConfiguration(
                $this->getTestBucketId(),
                $configurationOptions,
            );
            $this->fail('Table with invalid configurationId should not be created');
        } catch (ClientException $e) {
            $this->assertEquals('storage.validation.configurationNotFound', $e->getStringCode());
        }
    }

    public function testTableWithInvalidConfigurationContent(): void
    {
        // create test configuration
        $configuration = (new Configuration())
            ->setComponentId(self::CUSTOM_QUERY_MANAGER_COMPONENT_ID)
            ->setConfigurationId($this->configId)
            ->setName($this->configId)
            ->setConfiguration(['value' => 1]);
        $this->componentsClient->addConfiguration($configuration);

        try {
            // create table from config
            $tableName = 'custom-table-1';
            $configurationOptions = (new TableWithConfigurationOptions($tableName, $this->configId));
            $this->_client->createTableWithConfiguration(
                $this->getTestBucketId(),
                $configurationOptions,
            );
            $this->fail('Table with invalid configurationId should not be created');
        } catch (ClientException $e) {
            $this->assertEquals('storage.validation.invalidConfigurationForTables', $e->getStringCode());
        }
    }

    public function testTableWithInvalidQuery(): void
    {
        // create test configuration
        $configuration = (new Configuration())
            ->setComponentId(self::CUSTOM_QUERY_MANAGER_COMPONENT_ID)
            ->setConfigurationId($this->configId)
            ->setName($this->configId)
            ->setConfiguration([
                'parameters' => [
                    'migrations' => [
                        [
                            'sql' => 'CREATE TABLE {{ id(bucketName) }}.{{ id(tableName) }} (id integer, name varchar(100), [_timestamp] DATETIME2)',
                            'description' => 'first ever',
                        ],
                        [
                            'sql' => 'ASD',
                        ],
                    ],
                ],
            ]);
        $this->componentsClient->addConfiguration($configuration);

        try {
            // create table from config
            $tableName = 'custom-table-1';
            $configurationOptions = (new TableWithConfigurationOptions($tableName, $this->configId));
            $this->_client->createTableWithConfiguration(
                $this->getTestBucketId(),
                $configurationOptions,
            );
            $this->fail('Table with invalid query in configuration should result in exception');
        } catch (ClientException $e) {
            $errorMessage = 'Migration ID: 1 of custom table failed because of : An exception occurred while executing a query:';
            $this->assertStringContainsString($errorMessage, $e->getMessage());
            $this->assertStringContainsString('ASD', $e->getMessage());
        }

        // check events
        $assertCallback = function ($events) {
            $this->assertCount(1, $events);
            $event = reset($events);
            $this->assertArrayHasKey('params', $event);
            $this->assertArrayHasKey('executedQuery', $event['params']);
            $this->assertStringContainsString('CREATE TABLE', $event['params']['executedQuery']);
        };
        $objectId = $this->getTestBucketId() . '.custom-table-1';
        $query = new EventsQueryBuilder();
        $query->setEvent('storage.tableWithConfigurationMigrated')
            ->setTokenId($this->tokenId)
            ->setObjectId($objectId);
        $this->assertEventWithRetries($this->client, $assertCallback, $query);

        $assertCallback = function ($events) {
            $this->assertCount(1, $events);
            $event = reset($events);
            $this->assertArrayHasKey('params', $event);
            $this->assertArrayHasKey('executedQuery', $event['params']);
            $this->assertSame('ASD', $event['params']['executedQuery']);
        };
        $objectId = $this->getTestBucketId() . '.custom-table-1';
        $query = new EventsQueryBuilder();
        $query->setEvent('storage.tableWithConfigurationMigrationFailed')
            ->setTokenId($this->tokenId)
            ->setObjectId($objectId);
        $this->assertEventWithRetries($this->client, $assertCallback, $query);

        $assertCallback = function ($events) {
            $this->assertCount(1, $events);
        };
        $objectId = $this->getTestBucketId() . '.custom-table-1';
        $query = new EventsQueryBuilder();
        $query->setEvent('storage.tableWithConfigurationMigrationFailed')
            ->setTokenId($this->tokenId)
            ->setObjectId($objectId);
        $this->assertEventWithRetries($this->client, $assertCallback, $query);
    }

    public function testCreateAndDeleteTableWithMigration(): void
    {
        // create test configuration
        $configuration = (new Configuration())
            ->setComponentId(self::CUSTOM_QUERY_MANAGER_COMPONENT_ID)
            ->setConfigurationId($this->configId)
            ->setName($this->configId)
            ->setConfiguration([
                'parameters' => [
                    'migrations' => [
                        [
                            'sql' => 'CREATE TABLE {{ id(bucketName) }}.{{ id(tableName) }} (id integer, name varchar(100), [_timestamp] DATETIME2)',
                            'description' => 'first ever',
                        ],
                    ],
                ],
            ]);
        $this->componentsClient->addConfiguration($configuration);

        // create table from config
        $tableName = 'custom-table-1';
        $configurationOptions = (new TableWithConfigurationOptions($tableName, $this->configId));
        $tableId = $this->_client->createTableWithConfiguration(
            $this->getTestBucketId(),
            $configurationOptions,
        );

        try {
            $this->componentsClient->deleteConfiguration(self::CUSTOM_QUERY_MANAGER_COMPONENT_ID, $configuration->getConfigurationId());
            $this->fail('deleting configuration with table should fail');
        } catch (ClientException $e) {
            $this->assertSame(sprintf('Configuration cannot be deleted because it is being used in following configured tables: %s. Delete them first.', $tableId), $e->getMessage());
        }

        $this->client->dropTable($tableId);
        $this->componentsClient->deleteConfiguration(self::CUSTOM_QUERY_MANAGER_COMPONENT_ID, $configuration->getConfigurationId());
    }

    public function testCreateTwoTablesWithSameConfiguration(): void
    {
        // create test configuration
        $configuration = (new Configuration())
            ->setComponentId(self::CUSTOM_QUERY_MANAGER_COMPONENT_ID)
            ->setConfigurationId($this->configId)
            ->setName($this->configId)
            ->setConfiguration([
                'parameters' => [
                    'migrations' => [
                        [
                            'sql' => 'CREATE TABLE {{ id(bucketName) }}.{{ id(tableName) }} (id integer, name varchar(100), [_timestamp] DATETIME2)',
                            'description' => 'first ever',
                        ],
                    ],
                ],
            ]);
        $this->componentsClient->addConfiguration($configuration);

        // create table from config
        $tableName = 'custom-table-1';
        $configurationOptions = (new TableWithConfigurationOptions($tableName, $this->configId));
        $this->_client->createTableWithConfiguration(
            $this->getTestBucketId(),
            $configurationOptions,
        );

        try {
            $tableName = 'custom-table-2';
            $configurationOptions = (new TableWithConfigurationOptions($tableName, $this->configId));
            $this->_client->createTableWithConfiguration(
                $this->getTestBucketId(),
                $configurationOptions,
            );
            $this->fail('shouldn\'t be able to create table with same config');
        } catch (ClientException $e) {
            $this->assertSame('Configuration is used for another table already', $e->getMessage());
        }
    }
}
