<?php

namespace Keboola\Test\Common;

use Keboola\StorageApi\BranchAwareClient;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\Options\Components\Configuration;
use Keboola\StorageApi\Options\Components\ConfigurationRow;
use Keboola\StorageApi\Options\Components\ConfigurationState;
use Keboola\StorageApi\Options\Components\ListComponentsOptions;
use Keboola\Test\StorageApiTestCase;

class ComponentsEventsTest extends StorageApiTestCase
{
    const COMPONENT_ID = 'wr-db';
    const CONFIGURATION_NAME = 'component-events-test';

    /**
     * @var int
     * generated uid
     */
    private $configurationId;

    /**
     * @var Components
     */
    private $components;

    public function setUp()
    {
        parent::setUp();

        $this->components = new Components($this->_client);
        foreach ($this->components->listComponents() as $component) {
            foreach ($component['configurations'] as $configuration) {
                $this->components->deleteConfiguration($component['id'], $configuration['id']);
            }
        }

        // erase all deleted configurations
        foreach ($this->components->listComponents((new ListComponentsOptions())->setIsDeleted(true)) as $component) {
            foreach ($component['configurations'] as $configuration) {
                $this->components->deleteConfiguration($component['id'], $configuration['id']);
            }
        }

        // initialize variables
        $this->configurationId = $this->_client->generateId();
        $this->initEvents();
    }

    /**
     * @dataProvider provideComponentsClient
     */
    public function testConfigurationChange(callable $getClient)
    {
        /** @var Client $client */
        $client = $getClient($this);

        $components = new Components($client);
        $config = $this->getConfiguration();
        $components->addConfiguration($config);

        // test no change
        $components->updateConfiguration($config);
        $events = $this->listEvents($client, 'storage.componentConfigurationChanged');
        self::assertNotEquals('storage.componentConfigurationChanged', $events[0]['event']);

        $config->setDescription('new desc');
        $components->updateConfiguration($config);

        $events = $this->listEvents($client, 'storage.componentConfigurationChanged');
        $this->assertEvent(
            $events[0],
            'storage.componentConfigurationChanged',
            'Changed component configuration "component-events-test" (wr-db)',
            $this->configurationId,
            'component-events-test',
            'componentConfiguration',
            [
                'component' => 'wr-db',
                'configurationId' => $this->configurationId,
                'name' => 'component-events-test',
                'version' => 2,
            ]
        );
    }

    /**
     * @return Configuration
     */
    private function getConfiguration()
    {
        $config = (new Configuration())
            ->setComponentId(self::COMPONENT_ID)
            ->setConfigurationId($this->configurationId)
            ->setName(self::CONFIGURATION_NAME);
        return $config;
    }

    /**
     * @dataProvider provideComponentsClient
     */
    public function testConfigurationCreate(callable $getClient)
    {
        /** @var Client $client */
        $client = $getClient($this);
        $components = new Components($client);

        $config = $this->getConfiguration();
        $components->addConfiguration($config);

        // check create event
        $events = $this->listEvents($client, 'storage.componentConfigurationCreated');
        $this->assertEvent(
            $events[0],
            'storage.componentConfigurationCreated',
            'Created component configuration "component-events-test" (wr-db)',
            $config->getConfigurationId(),
            'component-events-test',
            'componentConfiguration',
            [
                'component' => 'wr-db',
                'configurationId' => $config->getConfigurationId(),
                'name' => 'component-events-test',
                'version' => 1,
            ]
        );

        // check restore event on create action
        $components->deleteConfiguration(self::COMPONENT_ID, $this->configurationId);
        $components->addConfiguration($this->getConfiguration());

        $events = $this->listEvents($client, 'storage.componentConfigurationRestored');
        $this->assertEvent(
            $events[0],
            'storage.componentConfigurationRestored',
            'Restored component configuration "component-events-test" (wr-db)',
            $this->configurationId,
            'component-events-test',
            'componentConfiguration',
            [
                'component' => 'wr-db',
                'configurationId' => $this->configurationId,
                'name' => 'component-events-test',
                'version' => 3,
            ]
        );
    }

    /**
     * @dataProvider provideComponentsClient
     */
    public function testConfigurationDelete(callable $getClient)
    {
        /** @var Client $client */
        $client = $getClient($this);
        $components = new Components($client);

        if ($client instanceof BranchAwareClient) {
            $this->markTestSkipped('Deleting configuration from trash is not allowed in development branches.');
        }

        $config = $this->getConfiguration();
        $components->addConfiguration($config);

        // delete
        $components->deleteConfiguration(self::COMPONENT_ID, $this->configurationId);
        $events = $this->listEvents($client, 'storage.componentConfigurationDeleted', $this->configurationId);
        $this->assertEvent(
            $events[0],
            'storage.componentConfigurationDeleted',
            'Deleted component configuration "component-events-test" (wr-db)',
            $this->configurationId,
            'component-events-test',
            'componentConfiguration',
            [
                'component' => 'wr-db',
                'configurationId' => $this->configurationId,
                'name' => 'component-events-test',
                'version' => 2,
            ]
        );

        // purge
        $components->deleteConfiguration(self::COMPONENT_ID, $this->configurationId);
        $events = $this->listEvents($client, 'storage.componentConfigurationPurged', $this->configurationId);
        $this->assertEvent(
            $events[0],
            'storage.componentConfigurationPurged',
            'Permanently deleted component configuration "component-events-test" (wr-db)',
            $this->configurationId,
            'component-events-test',
            'componentConfiguration',
            [
                'component' => 'wr-db',
                'configurationId' => $this->configurationId,
                'name' => 'component-events-test',
                'version' => 2,
            ]
        );
    }

    /**
     * @dataProvider provideComponentsClient
     */
    public function testConfigurationRestore(callable $getClient)
    {
        /** @var Client $client */
        $client = $getClient($this);
        $components = new Components($client);

        $config = $this->getConfiguration();
        $components->addConfiguration($config);
        $components->deleteConfiguration(self::COMPONENT_ID, $this->configurationId);

        $components->restoreComponentConfiguration(self::COMPONENT_ID, $this->configurationId);
        $events = $this->listEvents($client, 'storage.componentConfigurationRestored');

        $this->assertEvent(
            $events[0],
            'storage.componentConfigurationRestored',
            'Restored component configuration "component-events-test" (wr-db)',
            $this->configurationId,
            'component-events-test',
            'componentConfiguration',
            [
                'component' => 'wr-db',
                'configurationId' => $this->configurationId,
                'name' => 'component-events-test',
                'version' => 3,
            ]
        );
    }

    /**
     * @dataProvider provideComponentsClient
     */
    public function testConfigurationVersionCopy(callable $getClient)
    {
        /** @var Client $client */
        $client = $getClient($this);
        $components = new Components($client);

        $config = $this->getConfiguration();
        $components->addConfiguration($config);

        $newConfig = $components->createConfigurationFromVersion(
            self::COMPONENT_ID,
            $this->configurationId,
            1,
            'new'
        );

        $events = $this->listEvents($client, 'storage.componentConfigurationCopied');
        $this->assertEvent(
            $events[0],
            'storage.componentConfigurationCopied',
            'Copied component configuration "component-events-test" to "new" (wr-db)',
            $newConfig['id'],
            'new',
            'componentConfiguration',
            [
                'component' => 'wr-db',
                'configurationId' => $newConfig['id'],
                'name' => 'new',
                'version' => 1,
                'sourceConfiguration' => [
                    'component' => 'wr-db',
                    'configurationId' => $this->configurationId,
                    'name' => 'component-events-test',
                    'version' => 1,
                ],
            ]
        );
    }

    /**
     * @dataProvider provideComponentsClient
     */
    public function testConfigurationVersionRollback(callable $getClient)
    {
        /** @var Client $client */
        $client = $getClient($this);
        $components = new Components($client);

        $config = $this->getConfiguration();
        $components->addConfiguration($config);

        $components->createConfigurationFromVersion(self::COMPONENT_ID, $this->configurationId, 1, 'v2');

        $components->rollbackConfiguration(self::COMPONENT_ID, $this->configurationId, 1, 'rollback');

        $events = $this->listEvents($client, 'storage.componentConfigurationRolledBack');
        $this->assertEvent(
            $events[0],
            'storage.componentConfigurationRolledBack',
            'Rolled back component configuration "component-events-test" (wr-db)',
            $this->configurationId,
            'component-events-test',
            'componentConfiguration',
            [
                'component' => 'wr-db',
                'configurationId' => $this->configurationId,
                'name' => 'component-events-test',
                'version' => 2,
                'sourceConfiguration' => [
                    'component' => 'wr-db',
                    'configurationId' => $this->configurationId,
                    'name' => 'component-events-test',
                    'version' => 1,
                ],
            ]
        );
    }

    /**
     * @dataProvider provideComponentsClient
     */
    public function testRowsChange(callable $getClient)
    {
        /** @var Client $client */
        $client = $getClient($this);
        $components = new Components($client);

        $config = $this->getConfiguration();
        $components->addConfiguration($config);

        $rowOptions = $this->getConfigRowOptions();
        $components->addConfigurationRow($rowOptions);
        $rowOptions->setDescription('desc2');
        $rowResponse = $components->updateConfigurationRow($rowOptions);

        $events = $this->listEvents($client, 'storage.componentConfigurationRowChanged');
        $this->assertEvent(
            $events[0],
            'storage.componentConfigurationRowChanged',
            'Changed component configuration row "rowName" (wr-db)',
            $rowResponse['id'],
            'rowName',
            'componentConfigurationRow',
            [
                'rowId' => $rowResponse['id'],
                'name' => 'rowName',
                'version' => 2,
                'configuration' => [
                    'component' => 'wr-db',
                    'configurationId' => $this->configurationId,
                    'name' => 'component-events-test',
                    'version' => 3,
                ],
            ]
        );
    }

    /**
     * @return ConfigurationRow
     */
    private function getConfigRowOptions()
    {
        $rowId = $this->_client->generateId();
        $rowOptions = new ConfigurationRow($this->getConfiguration());
        $rowOptions->setRowId($rowId);
        $rowOptions->setName('rowName');
        $rowOptions->setDescription('desc1');
        return $rowOptions;
    }

    /**
     * @dataProvider provideComponentsClient
     */
    public function testRowsCreate(callable $getClient)
    {
        /** @var Client $client */
        $client = $getClient($this);
        $components = new Components($client);

        $config = $this->getConfiguration();
        $components->addConfiguration($config);

        $rowOptions = $this->getConfigRowOptions();
        $rowResponse = $components->addConfigurationRow($rowOptions);

        $events = $this->listEvents($client, 'storage.componentConfigurationRowCreated');
        $this->assertEvent(
            $events[0],
            'storage.componentConfigurationRowCreated',
            'Created component configuration row "rowName" (wr-db)',
            $rowResponse['id'],
            'rowName',
            'componentConfigurationRow',
            [
                'rowId' => $rowResponse['id'],
                'name' => 'rowName',
                'version' => 1,
                'configuration' => [
                    'component' => 'wr-db',
                    'configurationId' => $this->configurationId,
                    'name' => 'component-events-test',
                    'version' => 2,
                ],
            ]
        );
    }

    /**
     * @dataProvider provideComponentsClient
     */
    public function testRowsDelete(callable $getClient)
    {
        /** @var Client $client */
        $client = $getClient($this);
        $components = new Components($client);

        $config = $this->getConfiguration();
        $components->addConfiguration($config);

        $rowOptions = $this->getConfigRowOptions();
        $rowResponse = $components->addConfigurationRow($rowOptions);

        $components->deleteConfigurationRow(self::COMPONENT_ID, $this->configurationId, $rowOptions->getRowId());

        $events = $this->listEvents($client, 'storage.componentConfigurationRowDeleted');
        $this->assertEvent(
            $events[0],
            'storage.componentConfigurationRowDeleted',
            'Deleted component configuration row "rowName" (wr-db)',
            $rowResponse['id'],
            'rowName',
            'componentConfigurationRow',
            [
                'rowId' => $rowResponse['id'],
                'name' => 'rowName',
                'version' => 1,
                'configuration' => [
                    'component' => 'wr-db',
                    'configurationId' => $this->configurationId,
                    'name' => 'component-events-test',
                    'version' => 3,
                ],
            ]
        );
    }

    /**
     * @dataProvider provideComponentsClient
     */
    public function testRowsVersionCreate(callable $getClient)
    {
        /** @var Client $client */
        $client = $getClient($this);
        $components = new Components($client);

        $config = $this->getConfiguration();
        $components->addConfiguration($config);

        $rowOptions = $this->getConfigRowOptions();
        $sourceRowResponse = $components->addConfigurationRow($rowOptions);

        $rowResponse = $components->createConfigurationRowFromVersion(
            self::COMPONENT_ID,
            $this->configurationId,
            $rowOptions->getRowId(),
            1
        );

        $events = $this->listEvents($client, 'storage.componentConfigurationRowCopied');
        $this->assertEvent(
            $events[0],
            'storage.componentConfigurationRowCopied',
            'Copied component configuration row "rowName" (wr-db)',
            $rowResponse['id'],
            'rowName',
            'componentConfigurationRow',
            [
                'rowId' => $rowResponse['id'],
                'name' => 'rowName',
                'version' => 1,
                'configuration' => [
                    'component' => 'wr-db',
                    'configurationId' => $config->getConfigurationId(),
                    'name' => 'component-events-test',
                    'version' => 3,
                ],
                'sourceRow' => [
                    'rowId' => $sourceRowResponse['id'],
                    'name' => 'rowName',
                    'version' => 1,
                    'configuration' => [
                        'component' => 'wr-db',
                        'configurationId' => $config->getConfigurationId(),
                        'name' => 'component-events-test',
                        'version' => 2,
                    ],
                ],
            ]
        );
    }

    /**
     * @dataProvider provideComponentsClient
     */
    public function testRowsVersionRollback(callable $getClient)
    {
        /** @var Client $client */
        $client = $getClient($this);
        $components = new Components($client);

        $config = $this->getConfiguration();
        $components->addConfiguration($config);

        $rowOptions = $this->getConfigRowOptions();
        $rowToRollbackTo = $components->addConfigurationRow($rowOptions);

        $components->createConfigurationRowFromVersion(
            self::COMPONENT_ID,
            $this->configurationId,
            $rowOptions->getRowId(),
            1
        );

        $components->rollbackConfigurationRow(
            self::COMPONENT_ID,
            $this->configurationId,
            $rowOptions->getRowId(),
            1
        );

        $events = $this->listEvents($client, 'storage.componentConfigurationRowRolledBack');
        $this->assertEvent(
            $events[0],
            'storage.componentConfigurationRowRolledBack',
            'Rolled back component configuration row "rowName" (wr-db)',
            $rowToRollbackTo['id'],
            'rowName',
            'componentConfigurationRow',
            [
                'rowId' => $rowToRollbackTo['id'],
                'name' => 'rowName',
                'version' => 2,
                'configuration' => [
                    'component' => 'wr-db',
                    'configurationId' => $this->configurationId,
                    'name' => 'component-events-test',
                    'version' => 4,
                ],
                'sourceRow' => [
                        'rowId' => $rowToRollbackTo['id'],
                        'name' => 'rowName',
                        'version' => 1,
                        'configuration' => [
                            'component' => 'wr-db',
                            'configurationId' => $this->configurationId,
                            'name' => 'component-events-test',
                            'version' => 3,
                        ],
                ],
            ]
        );
    }
}
