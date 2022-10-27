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
use Keboola\Test\Utils\EventTesterUtils;

class MigrateTableWithConfigurationTest extends StorageApiTestCase
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
                $token['owner']['defaultBackend']
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

    public function testTableMigrate(): void
    {
        $tableName = 'custom-table-1';
        $tableId = $this->prepareTableWithConfiguration($tableName, [
            'migrations' => [
                [
                    'sql' => 'CREATE TABLE {{ id(bucketName) }}.{{ id(tableName) }} ("id" integer, "name" varchar(100))',
                    'description' => 'first ever',
                ],
            ],
        ]);

        $table = $this->_client->getTable($tableId);
        $this->assertArrayHasKey('configuration', $table);
        $this->assertArrayHasKey('migrationIndex', $table['configuration']);
        $this->assertSame(0, $table['configuration']['migrationIndex']);

        $configuration = (new Configuration())
            ->setComponentId(StorageApiTestCase::CUSTOM_QUERY_MANAGER_COMPONENT_ID)
            ->setConfigurationId($this->configId);

        $configurationArray = $this->componentsClient->getConfiguration(StorageApiTestCase::CUSTOM_QUERY_MANAGER_COMPONENT_ID, $this->configId);
        $configuration->setConfiguration([
            'migrations' => [
                ...$configurationArray['configuration']['migrations'],
                [
                    'sql' => 'ALTER TABLE {{ id(bucketName) }}.{{ id(tableName) }} DROP COLUMN "name"',
                    'description' => 'drop name',
                ],
                [
                    'sql' => 'ALTER TABLE {{ id(bucketName) }}.{{ id(tableName) }} ADD "name_another" VARCHAR(50)',
                    'description' => 'add name another',
                ],
            ],
        ]);
        $this->componentsClient->updateConfiguration($configuration);

        $this->_client->migrateTableWithConfiguration($tableId);

        $table = $this->_client->getTable($tableId);

        $this->assertArrayHasKey('configuration', $table);
        $this->assertArrayHasKey('migrationIndex', $table['configuration']);
        $this->assertSame(2, $table['configuration']['migrationIndex']);
        $this->assertEquals(['id', 'name_another'], $table['columns']);

        $this->assertTableColumnMetadata([
            'id' => [
                'KBC.datatype.type' => 'INT',
                'KBC.datatype.nullable' => '1',
                'KBC.datatype.basetype' => 'INTEGER',
            ],
            'name_another' => [
                'KBC.datatype.type' => 'VARCHAR',
                'KBC.datatype.nullable' => '1',
                'KBC.datatype.basetype' => 'STRING',
                'KBC.datatype.length' => '50',
            ],
        ], $table);

        // check events
        $events = $this->listEventsFilteredByName($this->client, 'storage.tableWithConfigurationMigrated', $tableId, 10);
        $this->assertCount(2, $events);
    }
}
