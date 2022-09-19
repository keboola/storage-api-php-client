<?php

namespace Keboola\Test\Backend\CommonPart1;

use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\Options\Components\Configuration;
use Keboola\StorageApi\Options\TableWithConfigurationOptions;
use Keboola\Test\ClientProvider\ClientProvider;
use Keboola\Test\StorageApiTestCase;
use Keboola\Test\Utils\EventTesterUtils;

class CreateTableWithConfigurationTest extends StorageApiTestCase
{
    use EventTesterUtils;

    public const COMPONENT_ID = 'keboola.app-custom-query-manager';
    protected string $configId;

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
                $token['owner']['defaultBackend']
            ));
        }

        // init buckets
        $this->initEmptyTestBucketsForParallelTests();

        $this->clientProvider = new ClientProvider($this);
        $this->client = $this->clientProvider->createClientForCurrentTest();

        // check component exists
        $this->componentsClient = new Components($this->client);
        $component = $this->componentsClient->getComponent(self::COMPONENT_ID);
        $this->assertEquals(self::COMPONENT_ID, $component['id']);

        $this->configId = sha1($this->generateDescriptionForTestObject());

        // delete configuration for this test
        try {
            $this->componentsClient->deleteConfiguration(self::COMPONENT_ID, $this->configId);
        } catch (ClientException $e) {
            if (preg_match('/Configuration cannot be deleted because it is being used in following configured tables: (.*). Delete them first./', $e->getMessage(), $out)) {
                $tablesToDelete = explode(', ', $out[1]);
                foreach ($tablesToDelete as $tableId) {
                    $this->client->dropTable($tableId);
                }
                $this->componentsClient->deleteConfiguration(self::COMPONENT_ID, $this->configId);
            } elseif (preg_match('/Configuration \w+ not found/', $e->getMessage(), $out)) {
                // noop, config already deleted
            } else {
                // throw other
                throw $e;
            }
        }

        $this->initEvents($this->client);
    }

    protected function assertMetadata(array $table): void
    {
        $expected = [
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
        ];
        $actual = [];
        foreach ($table['columnMetadata'] as $columnName => $metadatum) {
            $actual[$columnName] = [];
            foreach ($metadatum as $item) {
                $actual[$columnName][$item['key']] = $item['value'];
            }
        }
        $this->assertEquals($expected, $actual);
    }

    public function testTableCreate(): void
    {
        // create test configuration
        $configuration = (new Configuration())
            ->setComponentId(self::COMPONENT_ID)
            ->setConfigurationId($this->configId)
            ->setName($this->configId)
            ->setConfiguration([
                'migrations' => [
                    [
                        'sql' => 'CREATE TABLE {{ id(bucketName) }}.{{ id(tableName) }} ("id" integer, "name" varchar(100))',
                        'description' => 'first ever',
                    ],
                ],
            ]);
        $this->componentsClient->addConfiguration($configuration);

        // create table from config
        $tableName = 'custom-table-1';
        $configurationOptions = (new TableWithConfigurationOptions($tableName, $this->configId));
        $tableId = $this->_client->createTableWithConfiguration(
            $this->getTestBucketId(),
            $configurationOptions
        );

        $table = $this->_client->getTable($tableId);

        $this->assertEquals($tableId, $table['id']);
        $this->assertEquals($tableName, $table['name']);
        $this->assertEquals($tableName, $table['displayName'], 'display name is same as name');

        $this->assertTrue($table['isTyped']);
        $this->assertNotEmpty($table['created']);
        $this->assertEquals(['id', 'name'], $table['columns']);

        $this->assertMetadata($table);

        // check events
        $events = $this->listEventsFilteredByName($this->client, 'storage.tableWithConfigurationMigrated', $tableId, 10);
        $this->assertCount(1, $events);

        $events = $this->listEventsFilteredByName($this->client, 'storage.tableCreated', $tableId, 10);
        $this->assertCount(1, $events);
        $event = reset($events);
        $this->assertArrayHasKey('params', $event);
        $this->assertArrayHasKey('columns', $event['params']);
        $this->assertEqualsIgnoringCase(['id', 'name'], $event['params']['columns']);
    }

    public function testTableCreateWithMeaningFullQueryAsSecond(): void
    {
        // create test configuration
        $configuration = (new Configuration())
            ->setComponentId(self::COMPONENT_ID)
            ->setConfigurationId($this->configId)
            ->setName($this->configId)
            ->setConfiguration([
                'migrations' => [
                    [
                        'sql' => 'SELECT 1',
                        'description' => 'first ever',
                    ],
                    [
                        'sql' => 'CREATE TABLE {{ id(bucketName) }}.{{ id(tableName) }} (id integer, name varchar(100))',
                        'description' => 'second query',
                    ],
                ],
            ]);
        $this->componentsClient->addConfiguration($configuration);

        // create table from config
        $tableName = 'custom-table-1';
        $configurationOptions = (new TableWithConfigurationOptions($tableName, $this->configId));
        $tableId = $this->_client->createTableWithConfiguration(
            $this->getTestBucketId(),
            $configurationOptions
        );

        $table = $this->_client->getTable($tableId);

        $this->assertEquals($tableId, $table['id']);
        $this->assertEquals($tableName, $table['name']);
        $this->assertEquals($tableName, $table['displayName'], 'display name is same as name');
        $this->assertNotEmpty($table['created']);

        $this->assertMetadata($table);

        // check events
        $events = $this->listEventsFilteredByName($this->client, 'storage.tableWithConfigurationMigrated', $tableId, 10);
        $this->assertCount(2, $events);
        $event = end($events);
        $this->assertArrayHasKey('params', $event);
        $this->assertArrayHasKey('executedQuery', $event['params']);
        $this->assertSame('SELECT 1', $event['params']['executedQuery']);
        $event = prev($events);
        $this->assertArrayHasKey('params', $event);
        $this->assertArrayHasKey('executedQuery', $event['params']);
        $this->assertStringContainsString('CREATE TABLE', $event['params']['executedQuery']);

        $events = $this->listEventsFilteredByName($this->client, 'storage.tableCreated', $tableId, 10);
        $this->assertCount(1, $events);
    }

    public function testTableCreateWithToothLessQuery(): void
    {
        // create test configuration
        $configuration = (new Configuration())
            ->setComponentId(self::COMPONENT_ID)
            ->setConfigurationId($this->configId)
            ->setName($this->configId)
            ->setConfiguration([
                'migrations' => [
                    [
                        'sql' => 'SELECT 1',
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
            $configurationOptions
        );

        // check events
        $events = $this->listEventsFilteredByName($this->client, 'storage.tableWithConfigurationMigrated', null, 10);
        $this->assertCount(1, $events);

        $this->expectExceptionMessage('There were no events');
        $this->listEventsFilteredByName($this->client, 'storage.tableCreated', null, 10);
    }

    public function testTableWithUnsupportedCharactersInNameShouldNotBeCreated(): void
    {
        // create test configuration
        $configuration = (new Configuration())
            ->setComponentId(self::COMPONENT_ID)
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
                $configurationOptions
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
                $configurationOptions
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
            ->setComponentId(self::COMPONENT_ID)
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
                $configurationOptions
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
            ->setComponentId(self::COMPONENT_ID)
            ->setConfigurationId($this->configId)
            ->setName($this->configId)
            ->setConfiguration([
                'migrations' => [
                    [
                        'sql' => 'CREATE TABLE {{ id(bucketName) }}.{{ id(tableName) }} (id integer, name varchar(100))',
                        'description' => 'first ever',
                    ],
                    [
                        'sql' => 'ASD',
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
                $configurationOptions
            );
            $this->fail('Table with invalid query in configuration should result in exception');
        } catch (ClientException $e) {
            $errorMessage = 'Migration ID: 1 of custom table failed because of : An exception occurred while executing a query:';
            $this->assertStringContainsString($errorMessage, $e->getMessage());
            $this->assertStringContainsString('ASD', $e->getMessage());
        }

        // check events
        $events = $this->listEventsFilteredByName($this->client, 'storage.tableWithConfigurationMigrated', null, 10);
        $this->assertCount(1, $events);
        $event = reset($events);
        $this->assertArrayHasKey('params', $event);
        $this->assertArrayHasKey('executedQuery', $event['params']);
        $this->assertStringContainsString('CREATE TABLE', $event['params']['executedQuery']);

        $events = $this->listEventsFilteredByName($this->client, 'storage.tableWithConfigurationMigrationFailed', null, 10);
        $this->assertCount(1, $events);
        $event = reset($events);
        $this->assertArrayHasKey('params', $event);
        $this->assertArrayHasKey('executedQuery', $event['params']);
        $this->assertSame('ASD', $event['params']['executedQuery']);
    }

    public function testCreateAndDeleteTableWithMigration(): void
    {
        // create test configuration
        $configuration = (new Configuration())
            ->setComponentId(self::COMPONENT_ID)
            ->setConfigurationId($this->configId)
            ->setName($this->configId)
            ->setConfiguration([
                'migrations' => [
                    [
                        'sql' => 'CREATE TABLE {{ id(bucketName) }}.{{ id(tableName) }} (id integer, name varchar(100))',
                        'description' => 'first ever',
                    ],
                ],
            ]);
        $this->componentsClient->addConfiguration($configuration);

        // create table from config
        $tableName = 'custom-table-1';
        $configurationOptions = (new TableWithConfigurationOptions($tableName, $this->configId));
        $tableId = $this->_client->createTableWithConfiguration(
            $this->getTestBucketId(),
            $configurationOptions
        );

        try {
            $this->componentsClient->deleteConfiguration(self::COMPONENT_ID, $configuration->getConfigurationId());
            $this->fail('deleting configuration with table should fail');
        } catch (ClientException $e) {
            $this->assertSame(sprintf('Configuration cannot be deleted because it is being used in following configured tables: %s. Delete them first.', $tableId), $e->getMessage());
        }

        $this->client->dropTable($tableId);
        $this->componentsClient->deleteConfiguration(self::COMPONENT_ID, $configuration->getConfigurationId());
    }

    public function testCreateTwoTablesWithSameConfiguration(): void
    {
        // create test configuration
        $configuration = (new Configuration())
            ->setComponentId(self::COMPONENT_ID)
            ->setConfigurationId($this->configId)
            ->setName($this->configId)
            ->setConfiguration([
                'migrations' => [
                    [
                        'sql' => 'CREATE TABLE {{ id(bucketName) }}.{{ id(tableName) }} (id integer, name varchar(100))',
                        'description' => 'first ever',
                    ],
                ],
            ]);
        $this->componentsClient->addConfiguration($configuration);

        // create table from config
        $tableName = 'custom-table-1';
        $configurationOptions = (new TableWithConfigurationOptions($tableName, $this->configId));
        $this->_client->createTableWithConfiguration(
            $this->getTestBucketId(),
            $configurationOptions
        );

        try {
            $tableName = 'custom-table-2';
            $configurationOptions = (new TableWithConfigurationOptions($tableName, $this->configId));
            $this->_client->createTableWithConfiguration(
                $this->getTestBucketId(),
                $configurationOptions
            );
            $this->fail('shouldn\'t be able to create table with same config');
        } catch (ClientException $e) {
            $this->assertSame('Configuration is used for another table already', $e->getMessage());
        }
    }
}
