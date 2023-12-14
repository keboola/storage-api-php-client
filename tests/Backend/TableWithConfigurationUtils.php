<?php

declare(strict_types=1);

namespace Keboola\Test\Backend;

use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\Options\Components\Configuration;
use Keboola\StorageApi\Options\TableWithConfigurationOptions;
use Keboola\Test\StorageApiTestCase;

trait TableWithConfigurationUtils
{
    public static array $DEFAULT_CONFIGURATION_MIGRATIONS = [
        [
            'sql' => /** @lang TSQL */
                <<<SQL
                CREATE TABLE {{ id(bucketName) }}.{{ id(tableName) }} ([id] INTEGER, [NAME] VARCHAR(100))
                SQL,
            'description' => 'first ever',
        ],
    ];

    protected string $configId;

    public function dropTableAndConfiguration(string $configurationId): void
    {
        // delete configuration for this test
        try {
            $this->componentsClient->deleteConfiguration(StorageApiTestCase::CUSTOM_QUERY_MANAGER_COMPONENT_ID, $configurationId);
        } catch (ClientException $e) {
            $out = [];
            if (preg_match('/Configuration cannot be deleted because it is being used in following configured tables: (.*). Delete them first./', $e->getMessage(), $out)) {
                $tablesToDelete = explode(', ', $out[1]);
                foreach ($tablesToDelete as $tableId) {
                    $this->client->dropTable($tableId);
                }
                $this->componentsClient->deleteConfiguration(StorageApiTestCase::CUSTOM_QUERY_MANAGER_COMPONENT_ID, $configurationId);
            } elseif (preg_match('/Configuration \w+ not found/', $e->getMessage(), $out)) {
                // noop, config already deleted
            } else {
                // throw other
                throw $e;
            }
        }
    }

    public function assertComponentExists(): void
    {
        // check component exists
        $this->componentsClient = new Components($this->client);
        $component = $this->componentsClient->getComponent(StorageApiTestCase::CUSTOM_QUERY_MANAGER_COMPONENT_ID);
        $this->assertEquals(StorageApiTestCase::CUSTOM_QUERY_MANAGER_COMPONENT_ID, $component['id']);
    }

    /**
     * @param array<string, array<string, string>> $expected
     * @param array<string, array<string, array<string, array<string, string>>>> $table
     */
    protected function assertTableColumnMetadata(array $expected, array $table): void
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

    /**
     * @return array{0: string, 1: Configuration}
     * @throws \JsonException
     */
    public function createTableWithConfiguration(
        string $json,
        string $tableName,
        string $queriesOverrideType,
        array $migrations = null
    ): array {
        /** @var array{output:array} $jsonDecoded */
        $jsonDecoded = json_decode(
            $json,
            true,
            512,
            JSON_THROW_ON_ERROR | JSON_PRETTY_PRINT,
        );

        $queriesOverride = [];
        $queriesOverride[$queriesOverrideType] = $jsonDecoded['output'];

        $configuration = (new Configuration())
            ->setComponentId(StorageApiTestCase::CUSTOM_QUERY_MANAGER_COMPONENT_ID)
            ->setConfigurationId($this->configId)
            ->setName($this->configId)
            ->setConfiguration([
                'parameters' => [
                    'migrations' => $migrations ?? self::$DEFAULT_CONFIGURATION_MIGRATIONS,
                    'queriesOverride' => $queriesOverride,
                ],
            ]);

        return [
            $this->prepareTableWithConfiguration($tableName, $configuration),
            $configuration,
        ];
    }

    private function prepareTableWithConfiguration(string $tableName, Configuration $configuration): string
    {
        // create test configuration
        $this->componentsClient->addConfiguration($configuration);

        $configurationOptions = (new TableWithConfigurationOptions($tableName, $this->configId));
        $tableId = $this->_client->createTableWithConfiguration(
            $this->getTestBucketId(),
            $configurationOptions,
        );
        $this->initEvents($this->_client);
        return $tableId;
    }

    private function getDefaultConfiguration(): Configuration
    {
        return (new Configuration())
            ->setComponentId(StorageApiTestCase::CUSTOM_QUERY_MANAGER_COMPONENT_ID)
            ->setConfigurationId($this->configId)
            ->setName($this->configId)
            ->setConfiguration([
                'parameters' => [
                    'migrations' => [
                        [
                            'sql' => 'CREATE TABLE {{ id(bucketName) }}.{{ id(tableName) }} ("id" integer, "name" varchar(100))',
                            'description' => 'first ever',
                        ],
                    ],
                ],
            ]);
    }

    private function checkFeatureAndBackend(): void
    {
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
    }
}
