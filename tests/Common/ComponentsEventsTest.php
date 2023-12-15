<?php

namespace Keboola\Test\Common;

use Keboola\StorageApi\Client;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\Options\Components\Configuration;
use Keboola\StorageApi\Options\Components\ConfigurationRow;
use Keboola\StorageApi\Options\Components\ListComponentsOptions;
use Keboola\Test\ClientProvider\ClientProvider;
use Keboola\Test\StorageApiTestCase;
use Keboola\Test\Utils\EventsQueryBuilder;
use Keboola\Test\Utils\EventTesterUtils;
use PHPUnit\Framework\ExpectationFailedException;

class ComponentsEventsTest extends StorageApiTestCase
{
    use EventTesterUtils;

    const COMPONENT_ID = 'wr-db';
    const CONFIGURATION_NAME = 'component-events-test';

    /**
     * @var string generated uid
     * @phpstan-var numeric-string
     */
    private $configurationId;

    /**
     * @var Components
     */
    private $components;

    /**
     * @var Client
     */
    private $client;

    public function setUp(): void
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

        $clientProvider = new ClientProvider($this);
        $this->client = $clientProvider->createClientForCurrentTest();

        $this->initEvents($this->client);
    }

    /**
     * @dataProvider provideComponentsClientType
     */
    public function testConfigurationChange(): void
    {
        $components = new Components($this->client);
        $config = $this->getConfiguration();
        $components->addConfiguration($config);

        // test no change
        $components->updateConfiguration($config);

        $this->createAndWaitForEvent((new \Keboola\StorageApi\Event())->setComponent('dummy')->setMessage('dummy'));

        // wait for event or will fail
        try {
            $this->listEvents($this->client, 'storage.componentConfigurationChanged');
            $this->fail('Should fail');
        } catch (ExpectationFailedException $e) {
            $this->assertStringContainsString('Event does not match', $e->getMessage());
        }

        $config->setDescription('new desc');
        $components->updateConfiguration($config);

        $assertCallback = function ($events) {
            $this->assertCount(1, $events);
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
                ],
            );
        };
        $query = new EventsQueryBuilder();
        $query->setEvent('storage.componentConfigurationChanged')
            ->setTokenId($this->tokenId);
        $this->assertEventWithRetries($this->client, $assertCallback, $query);
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
     * @dataProvider provideComponentsClientType
     */
    public function testConfigurationCreate(): void
    {
        $components = new Components($this->client);

        $config = $this->getConfiguration();
        $components->addConfiguration($config);

        // check create event
        $assertCallback = function ($events) use ($config) {
            $this->assertCount(1, $events);
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
                ],
            );
        };
        $query = new EventsQueryBuilder();
        $query->setEvent('storage.componentConfigurationCreated')
            ->setTokenId($this->tokenId);
        $this->assertEventWithRetries($this->client, $assertCallback, $query);

        // check restore event on create action
        $components->deleteConfiguration(self::COMPONENT_ID, $this->configurationId);
        $components->addConfiguration($this->getConfiguration());

        $assertCallback = function ($events) {
            $this->assertCount(1, $events);
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
                ],
            );
        };

        $query = new EventsQueryBuilder();
        $query->setEvent('storage.componentConfigurationRestored')
            ->setTokenId($this->tokenId);
        $this->assertEventWithRetries($this->client, $assertCallback, $query);
    }

    /**
     * @dataProvider provideComponentsClientType
     * @param string $clientType
     */
    public function testConfigurationDelete($clientType): void
    {
        $components = new Components($this->client);

        $config = $this->getConfiguration();
        $components->addConfiguration($config);

        // delete
        $components->deleteConfiguration(self::COMPONENT_ID, $this->configurationId);

        $assertCallback = function ($events) {
            $this->assertCount(1, $events);
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
                ],
            );
        };

        $query = new EventsQueryBuilder();
        $query->setEvent('storage.componentConfigurationDeleted')
            ->setTokenId($this->tokenId);
        $this->assertEventWithRetries($this->client, $assertCallback, $query);

        if ($clientType === ClientProvider::DEV_BRANCH) {
            $this->markTestSkipped('Deleting configuration from trash is not allowed in development branches.');
        }

        // purge
        $components->deleteConfiguration(self::COMPONENT_ID, $this->configurationId);
        $assertCallback = function ($events) {
            $this->assertCount(1, $events);
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
                ],
            );
        };

        $query = new EventsQueryBuilder();
        $query->setEvent('storage.componentConfigurationPurged')
            ->setTokenId($this->tokenId);
        $this->assertEventWithRetries($this->client, $assertCallback, $query);
    }

    /**
     * @dataProvider provideComponentsClientType
     */
    public function testConfigurationRestore(): void
    {
        $components = new Components($this->client);

        $config = $this->getConfiguration();
        $components->addConfiguration($config);
        $components->deleteConfiguration(self::COMPONENT_ID, $this->configurationId);

        $components->restoreComponentConfiguration(self::COMPONENT_ID, $this->configurationId);
        $assertCallback = function ($events) {
            $this->assertCount(1, $events);
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
                ],
            );
        };
        $query = new EventsQueryBuilder();
        $query->setEvent('storage.componentConfigurationRestored')
            ->setTokenId($this->tokenId);
        $this->assertEventWithRetries($this->client, $assertCallback, $query);
    }

    /**
     * @dataProvider provideComponentsClientType
     */
    public function testConfigurationVersionCopy(): void
    {
        $components = new Components($this->client);

        $config = $this->getConfiguration();
        $components->addConfiguration($config);

        $newConfig = $components->createConfigurationFromVersion(
            self::COMPONENT_ID,
            $this->configurationId,
            1,
            'new',
        );

        $assertCallback = function ($events) use ($newConfig) {
            $this->assertCount(1, $events);
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
                ],
            );
        };
        $query = new EventsQueryBuilder();
        $query->setEvent('storage.componentConfigurationCopied')
            ->setTokenId($this->tokenId);
        $this->assertEventWithRetries($this->client, $assertCallback, $query);
    }

    /**
     * @dataProvider provideComponentsClientType
     */
    public function testConfigurationVersionRollback(): void
    {
        $components = new Components($this->client);

        $config = $this->getConfiguration();
        $components->addConfiguration($config);

        $components->createConfigurationFromVersion(self::COMPONENT_ID, $this->configurationId, 1, 'v2');

        $components->rollbackConfiguration(self::COMPONENT_ID, $this->configurationId, 1, 'rollback');
        $assertCallback = function ($events) {
            $this->assertCount(1, $events);
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
                ],
            );
        };
        $query = new EventsQueryBuilder();
        $query->setEvent('storage.componentConfigurationRolledBack')
            ->setTokenId($this->tokenId);
        $this->assertEventWithRetries($this->client, $assertCallback, $query);
    }

    /**
     * @dataProvider provideComponentsClientType
     */
    public function testRowsChange(): void
    {
        $components = new Components($this->client);

        $config = $this->getConfiguration();
        $components->addConfiguration($config);

        $rowOptions = $this->getConfigRowOptions();
        $components->addConfigurationRow($rowOptions);
        $rowOptions->setDescription('desc2');
        $rowResponse = $components->updateConfigurationRow($rowOptions);

        $assertCallback = function ($events) use ($rowResponse) {
            $this->assertCount(1, $events);
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
                ],
            );
        };

        $query = new EventsQueryBuilder();
        $query->setEvent('storage.componentConfigurationRowChanged')
            ->setTokenId($this->tokenId);
        $this->assertEventWithRetries($this->client, $assertCallback, $query);
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
     * @dataProvider provideComponentsClientType
     */
    public function testRowsCreate(): void
    {
        $components = new Components($this->client);

        $config = $this->getConfiguration();
        $components->addConfiguration($config);

        $rowOptions = $this->getConfigRowOptions();
        $rowResponse = $components->addConfigurationRow($rowOptions);

        $assertCallback = function ($events) use ($rowResponse) {
            $this->assertCount(1, $events);
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
                ],
            );
        };
        $query = new EventsQueryBuilder();
        $query->setEvent('storage.componentConfigurationRowCreated')
            ->setTokenId($this->tokenId);
        $this->assertEventWithRetries($this->client, $assertCallback, $query);
    }

    /**
     * @dataProvider provideComponentsClientType
     */
    public function testRowsDelete(): void
    {
        $components = new Components($this->client);

        $config = $this->getConfiguration();
        $components->addConfiguration($config);

        $rowOptions = $this->getConfigRowOptions();
        $rowResponse = $components->addConfigurationRow($rowOptions);

        $components->deleteConfigurationRow(self::COMPONENT_ID, $this->configurationId, $rowOptions->getRowId());

        $assertCallback = function ($events) use ($rowResponse) {
            $this->assertCount(1, $events);
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
                ],
            );
        };
        $query = new EventsQueryBuilder();
        $query->setEvent('storage.componentConfigurationRowDeleted')
            ->setTokenId($this->tokenId);
        $this->assertEventWithRetries($this->client, $assertCallback, $query);
    }

    /**
     * @dataProvider provideComponentsClientType
     */
    public function testRowsVersionCreate(): void
    {
        $components = new Components($this->client);

        $config = $this->getConfiguration();
        $components->addConfiguration($config);

        $rowOptions = $this->getConfigRowOptions();
        $sourceRowResponse = $components->addConfigurationRow($rowOptions);

        $rowResponse = $components->createConfigurationRowFromVersion(
            self::COMPONENT_ID,
            $this->configurationId,
            $rowOptions->getRowId(),
            1,
        );
        $assertCallback = function ($events) use ($rowResponse, $config, $sourceRowResponse) {
            $this->assertCount(1, $events);
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
                ],
            );
        };
        $query = new EventsQueryBuilder();
        $query->setEvent('storage.componentConfigurationRowCopied')
            ->setTokenId($this->tokenId);
        $this->assertEventWithRetries($this->client, $assertCallback, $query);
    }

    /**
     * @dataProvider provideComponentsClientType
     */
    public function testRowsVersionRollback(): void
    {
        $components = new Components($this->client);

        $config = $this->getConfiguration();
        $components->addConfiguration($config);

        $rowOptions = $this->getConfigRowOptions();
        $rowToRollbackTo = $components->addConfigurationRow($rowOptions);

        $components->createConfigurationRowFromVersion(
            self::COMPONENT_ID,
            $this->configurationId,
            $rowOptions->getRowId(),
            1,
        );

        $components->rollbackConfigurationRow(
            self::COMPONENT_ID,
            $this->configurationId,
            $rowOptions->getRowId(),
            1,
        );

        $assertCallback = function ($events) use ($rowToRollbackTo) {
            $this->assertCount(1, $events);
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
                ],
            );
        };

        $query = new EventsQueryBuilder();
        $query->setEvent('storage.componentConfigurationRowRolledBack')
            ->setTokenId($this->tokenId);
        $this->assertEventWithRetries($this->client, $assertCallback, $query);
    }
}
