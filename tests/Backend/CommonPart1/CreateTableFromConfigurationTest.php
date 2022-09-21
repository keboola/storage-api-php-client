<?php

namespace Keboola\Test\Backend\CommonPart1;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\Options\Components\Configuration;
use Keboola\Test\ClientProvider\ClientProvider;
use Keboola\Test\StorageApiTestCase;

class CreateTableFromConfigurationTest extends StorageApiTestCase
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
        if (!in_array('tables-from-configuration', $token['owner']['features'])) {
            $this->markTestSkipped(sprintf('Creating tables from configurations feature is not enabled for project "%s"', $token['owner']['id']));
        }

        // init configurations
        $this->cleanupConfigurations();
        $this->clientProvider = new ClientProvider($this);
        $this->client = $this->clientProvider->createClientForCurrentTest();

        // check component exists
        $this->componentsClient = new Components($this->client);
        $component = $this->componentsClient->getComponent(self::COMPONENT_ID);
        $this->assertEquals(self::COMPONENT_ID, $component['id']);

        // init buckets
        $this->initEmptyTestBucketsForParallelTests();
    }

    public function testTableCreate(): void
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
            ->setDescription('Configuration for Custom Queries')
        ;
        $this->componentsClient->addConfiguration($configuration);

        // create table from config
        $tableName = 'custom-table-1';
        $tableId = $this->_client->createTableFromConfiguration(
            $this->getTestBucketId(self::STAGE_IN),
            [
                'name' => $tableName,
                'configurationId' => $configurationId,
            ],
        );
        // DEBUG for now return full response with jobParameters
        $branchId = $this->getDefaultBranchId($this);
        $this->assertSame([
            'name' => $tableName,
            'configurationId' => $configurationId,
            'branchId' => $branchId,
            'componentId' => $componentId,
        ], $tableId);

        $this->markTestIncomplete('Complete while service is ready');
        // TODO after service is ready check result
        $table = $this->_client->getTable($tableId);
        $this->assertArrayHasKey('displayName', $table['bucket']);

        $this->assertEquals($tableId, $table['id']);
        $this->assertEquals($tableName, $table['name']);
        $this->assertEquals($tableName, $table['displayName'], 'display name is same as name');
        $this->assertNotEmpty($table['created']);
        $this->assertNotEmpty($table['lastChangeDate']);
        $this->assertNotEmpty($table['lastImportDate']);
        $this->assertEmpty($table['indexedColumns']);
        $this->assertNotEquals('0000-00-00 00:00:00', $table['created']);
        $this->assertNotEmpty($table['dataSizeBytes']);
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
            ->setDescription('Configuration for Custom Queries')
        ;
        $this->componentsClient->addConfiguration($configuration);

        try {
            // create table from config
            $tableName = 'custom.table.1';
            $tableId = $this->_client->createTableFromConfiguration(
                $this->getTestBucketId(self::STAGE_IN),
                [
                    'name' => $tableName,
                    'configurationId' => $configurationId,
                ],
            );
            $this->fail('Table with dot in name should not be created');
        } catch (ClientException $e) {
            $this->assertEquals('validation.failed', $e->getStringCode());
        }
    }

    public function testTableWithInvalidConfigurationIdShouldNotBeCreated(): void
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
            ->setDescription('Configuration for Custom Queries')
        ;
        $this->componentsClient->addConfiguration($configuration);

        try {
            // create table from config
            $tableName = 'custom-table-1';
            $tableId = $this->_client->createTableFromConfiguration(
                $this->getTestBucketId(self::STAGE_IN),
                [
                    'name' => $tableName,
                    'configurationId' => 'config-not-exists',
                ],
            );
            $this->fail('Table with invalid configurationId should not be created');
        } catch (ClientException $e) {
            $this->assertEquals('storage.validation.configurationNotFound', $e->getStringCode());
        }
    }

    public function testMigrateTable(): void
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
            ->setDescription('Configuration for Custom Queries')
        ;
        $this->componentsClient->addConfiguration($configuration);

        // create table from config
        $tableName = 'custom-table-1';
        $tableId = $this->_client->createTableFromConfiguration(
            $this->getTestBucketId(self::STAGE_IN),
            [
                'name' => $tableName,
                'configurationId' => $configurationId,
            ],
        );


        $this->markTestIncomplete('Complete while creating service is ready');
        // TODO after service is ready check result
        $table = $this->_client->getTable($tableId);
        $this->assertEquals($tableId, $table['id']);

        // migrate table
        $this->markTestIncomplete('Complete while migration service is ready');
        $this->_client->migrateTableFromConfiguration($tableId);

        // TODO check migration result
    }

    public function testMigrateTableWithNotExistingConfiguration(): void
    {
        // create standard table without configuration
        $importFile = __DIR__ . '/../../_data/languages.csv';
        $tableName = 'custom-table-1';
        $tableId = $this->_client->createTableAsync(
            $this->getTestBucketId(self::STAGE_IN),
            $tableName,
            new CsvFile($importFile),
        );

        // migrate table
        try {
            $this->_client->migrateTableFromConfiguration($tableId);
            $this->fail('Table with not existing configurationId should not be migrated');
        } catch (ClientException $e) {
            $this->assertEquals('storage.validation.configurationNotFound', $e->getStringCode());
        }
    }
}
