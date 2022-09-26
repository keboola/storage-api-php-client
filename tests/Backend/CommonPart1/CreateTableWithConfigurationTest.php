<?php

namespace Keboola\Test\Backend\CommonPart1;

use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\Options\Components\Configuration;
use Keboola\StorageApi\Options\TableWithConfigurationOptions;
use Keboola\Test\ClientProvider\ClientProvider;
use Keboola\Test\StorageApiTestCase;

class CreateTableWithConfigurationTest extends StorageApiTestCase
{
    public const COMPONENT_ID = 'keboola.app-custom-query-manager';

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

        // init buckets
        $this->initEmptyTestBucketsForParallelTests();

        $this->clientProvider = new ClientProvider($this);
        $this->client = $this->clientProvider->createClientForCurrentTest();

        // init configurations
        try {
            $this->cleanupConfigurations();
        } catch (ClientException $e) {
            if (preg_match('/Configuration cannot be deleted because it is being used in following configured tables: (.*). Delete them first./', $e->getMessage(), $out)) {
                $tablesToDelete = explode(', ', $out[1]);
                foreach ($tablesToDelete as $tableId) {
                    $this->client->dropTable($tableId);
                }
            }
            $this->cleanupConfigurations();
        }

        // check component exists
        $this->componentsClient = new Components($this->client);
        $component = $this->componentsClient->getComponent(self::COMPONENT_ID);
        $this->assertEquals(self::COMPONENT_ID, $component['id']);
    }

    public function testTableCreate(): void
    {
        $configurationId = 'main-1';

        // create test configuration
        $configuration = (new Configuration())
            ->setComponentId(self::COMPONENT_ID)
            ->setConfigurationId($configurationId)
            ->setName('Main')
            ->setState(['stateValue' => 'some-value'])
            ->setConfiguration([
                'migrations' => [
                    [
                        'sql' => 'CREATE TABLE {{bucketName}}.{{tableName}} (id integer, name varchar(100))',
                        'description' => 'first ever',
                    ],
                ],
            ])
            ->setDescription('Configuration for Custom Queries');
        $this->componentsClient->addConfiguration($configuration);

        // create table from config
        $tableName = 'custom-table-1';
        $configurationOptions = (new TableWithConfigurationOptions())
            ->setTablename($tableName)
            ->setConfigurationId($configurationId);
        $tableId = $this->_client->createTableWithConfiguration(
            $this->getTestBucketId(),
            $configurationOptions
        );

        $table = $this->_client->getTable($tableId);
        $this->assertArrayHasKey('displayName', $table['bucket']);

        $this->assertEquals($tableId, $table['id']);
        $this->assertEquals($tableName, $table['name']);
        $this->assertEquals($tableName, $table['displayName'], 'display name is same as name');
        $this->assertNotEmpty($table['created']);
        $this->assertNotEquals('0000-00-00 00:00:00', $table['created']);
    }

    public function testTableCreateWithMeaningFullQueryAsSecond(): void
    {
        $configurationId = 'main-1';

        // create test configuration
        $configuration = (new Configuration())
            ->setComponentId(self::COMPONENT_ID)
            ->setConfigurationId($configurationId)
            ->setName('Main')
            ->setState(['stateValue' => 'some-value'])
            ->setConfiguration([
                'migrations' => [
                    [
                        'sql' => 'SELECT 1',
                        'description' => 'first ever',
                    ],
                    [
                        'sql' => 'CREATE TABLE {{bucketName}}.{{tableName}} (id integer, name varchar(100))',
                        'description' => 'first ever',
                    ],
                ],
            ])
            ->setDescription('Configuration for Custom Queries');
        $this->componentsClient->addConfiguration($configuration);

        // create table from config
        $tableName = 'custom-table-1';
        $configurationOptions = (new TableWithConfigurationOptions())
            ->setTablename($tableName)
            ->setConfigurationId($configurationId);
        $tableId = $this->_client->createTableWithConfiguration(
            $this->getTestBucketId(),
            $configurationOptions
        );

        $table = $this->_client->getTable($tableId);
        $this->assertArrayHasKey('displayName', $table['bucket']);

        $this->assertEquals($tableId, $table['id']);
        $this->assertEquals($tableName, $table['name']);
        $this->assertEquals($tableName, $table['displayName'], 'display name is same as name');
        $this->assertNotEmpty($table['created']);
        $this->assertNotEquals('0000-00-00 00:00:00', $table['created']);
    }

    public function testTableCreateWithToothLessQuery(): void
    {
        $configurationId = 'main-1';

        // create test configuration
        $configuration = (new Configuration())
            ->setComponentId(self::COMPONENT_ID)
            ->setConfigurationId($configurationId)
            ->setName('Main')
            ->setState(['stateValue' => 'some-value'])
            ->setConfiguration([
                'migrations' => [
                    [
                        'sql' => 'SELECT 1',
                    ],
                ],
            ])
            ->setDescription('Configuration for Custom Queries');
        $this->componentsClient->addConfiguration($configuration);

        // create table from config
        $tableName = 'custom-table-1';
        self::expectExceptionMessage('Configuration did not create any table');
        $configurationOptions = (new TableWithConfigurationOptions())
            ->setTablename($tableName)
            ->setConfigurationId($configurationId);
        $this->_client->createTableWithConfiguration(
            $this->getTestBucketId(),
            $configurationOptions
        );
    }

    public function testTableWithUnsupportedCharactersInNameShouldNotBeCreated(): void
    {
        $componentId = self::COMPONENT_ID;
        $configurationId = 'main-1';

        // create test configuration
        $configuration = (new Configuration())
            ->setComponentId($componentId)
            ->setConfigurationId($configurationId)
            ->setName('Main')
            ->setState(['stateValue' => 'some-value'])
            ->setConfiguration(['value' => 1])
            ->setDescription('Configuration for Custom Queries');
        $this->componentsClient->addConfiguration($configuration);

        try {
            // create table from config
            $tableName = 'custom.table.1';
            $configurationOptions = (new TableWithConfigurationOptions())
                ->setTablename($tableName)
                ->setConfigurationId($configurationId);
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
            $configurationOptions = (new TableWithConfigurationOptions())
                ->setTablename($tableName)
                ->setConfigurationId('doesNotExist');
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
        $componentId = self::COMPONENT_ID;
        $configurationId = 'main-1';

        // create test configuration
        $configuration = (new Configuration())
            ->setComponentId($componentId)
            ->setConfigurationId($configurationId)
            ->setName('Main')
            ->setState(['stateValue' => 'some-value'])
            ->setConfiguration(['value' => 1])
            ->setDescription('Configuration for Custom Queries');
        $this->componentsClient->addConfiguration($configuration);

        try {
            // create table from config
            $tableName = 'custom-table-1';
            $configurationOptions = (new TableWithConfigurationOptions())
                ->setTablename($tableName)
                ->setConfigurationId($configurationId);
            $this->_client->createTableWithConfiguration(
                $this->getTestBucketId(),
                $configurationOptions
            );
            $this->fail('Table with invalid configurationId should not be created');
        } catch (ClientException $e) {
            $this->assertEquals('storage.validation.invalidConfigurationForTables', $e->getStringCode());
        }
    }

    public function testCreateAndDeleteTableWithMigration()
    {
        $configurationId = 'main-1';

        // create test configuration
        $configuration = (new Configuration())
            ->setComponentId(self::COMPONENT_ID)
            ->setConfigurationId($configurationId)
            ->setName('Main')
            ->setState(['stateValue' => 'some-value'])
            ->setConfiguration([
                'migrations' => [
                    [
                        'sql' => 'CREATE TABLE {{bucketName}}.{{tableName}} (id integer, name varchar(100))',
                        'description' => 'first ever',
                    ],
                ],
            ])
            ->setDescription('Configuration for Custom Queries');
        $this->componentsClient->addConfiguration($configuration);

        // create table from config
        $tableName = 'custom-table-1';
        $configurationOptions = (new TableWithConfigurationOptions())
            ->setTablename($tableName)
            ->setConfigurationId($configurationId);
        $tableId = $this->_client->createTableWithConfiguration(
            $this->getTestBucketId(),
            $configurationOptions
        );

        $table = $this->_client->getTable($tableId);
        $this->assertArrayHasKey('displayName', $table['bucket']);

        try {
            $this->componentsClient->deleteConfiguration(self::COMPONENT_ID, $configuration->getConfigurationId());
            $this->fail('deleting configuration with table should fail');
        } catch (ClientException $e) {
            $this->assertSame(sprintf('Configuration cannot be deleted because it is being used in following configured tables: %s. Delete them first.', $tableId), $e->getMessage());
        }

        $this->client->dropTable($tableId);
        $this->componentsClient->deleteConfiguration(self::COMPONENT_ID, $configuration->getConfigurationId());
    }

    public function testCreateTwoTablesWithSameConfiguration()
    {
        $configurationId = 'main-1';

        // create test configuration
        $configuration = (new Configuration())
            ->setComponentId(self::COMPONENT_ID)
            ->setConfigurationId($configurationId)
            ->setName('Main')
            ->setState(['stateValue' => 'some-value'])
            ->setConfiguration([
                'migrations' => [
                    [
                        'sql' => 'CREATE TABLE {{bucketName}}.{{tableName}} (id integer, name varchar(100))',
                        'description' => 'first ever',
                    ],
                ],
            ])
            ->setDescription('Configuration for Custom Queries');
        $this->componentsClient->addConfiguration($configuration);

        // create table from config
        $tableName = 'custom-table-1';
        $configurationOptions = (new TableWithConfigurationOptions())
            ->setTablename($tableName)
            ->setConfigurationId($configurationId);
        $this->_client->createTableWithConfiguration(
            $this->getTestBucketId(),
            $configurationOptions
        );

        try {
            $tableName = 'custom-table-2';
            $configurationOptions = (new TableWithConfigurationOptions())
                ->setTablename($tableName)
                ->setConfigurationId($configurationId);
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
