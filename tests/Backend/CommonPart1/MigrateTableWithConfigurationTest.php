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

class MigrateTableWithConfigurationTest extends StorageApiTestCase
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

    protected function assertMetadata(array $table, array $expected): void
    {
        $actual = [];
        foreach ($table['columnMetadata'] as $columnName => $metadatum) {
            $actual[$columnName] = [];
            foreach ($metadatum as $item) {
                $actual[$columnName][$item['key']] = $item['value'];
            }
        }
        $this->assertEquals($expected, $actual);
    }

    public function testTableMigrate(): void
    {
        $tableName = 'custom-table-1';
        $tableId = $this->prepareTableWithConfiguration($tableName);

        $configuration = (new Configuration())
            ->setComponentId(self::COMPONENT_ID)
            ->setConfigurationId($this->configId);

        $configurationArray = $this->componentsClient->getConfiguration(self::COMPONENT_ID, $this->configId);
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

        $this->assertEquals(['id', 'name_another'], $table['columns']);

        $this->assertMetadata($table, [
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
        ]);

        // check events
        $events = $this->listEventsFilteredByName($this->client, 'storage.tableWithConfigurationMigrated', $tableId, 10);
        $this->assertCount(2, $events);
    }

    private function prepareTableWithConfiguration(string $tableName): string
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

        $configurationOptions = (new TableWithConfigurationOptions($tableName, $this->configId));
        $tableId = $this->_client->createTableWithConfiguration(
            $this->getTestBucketId(),
            $configurationOptions
        );
        $this->initEvents($this->_client);
        return $tableId;
    }
}
