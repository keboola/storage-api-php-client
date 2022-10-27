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

    private function prepareTableWithConfiguration(string $tableName, Configuration $configuration): string
    {
        // create test configuration
        $this->componentsClient->addConfiguration($configuration);

        $configurationOptions = (new TableWithConfigurationOptions($tableName, $this->configId));
        $tableId = $this->_client->createTableWithConfiguration(
            $this->getTestBucketId(),
            $configurationOptions
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
                'migrations' => [
                    [
                        'sql' => 'CREATE TABLE {{ id(bucketName) }}.{{ id(tableName) }} ("id" integer, "name" varchar(100))',
                        'description' => 'first ever',
                    ],
                ],
            ]);
    }
}
