<?php

namespace Keboola\Test\Common;

use Keboola\StorageApi\Components;
use Keboola\StorageApi\Options\Components\Configuration;
use Keboola\StorageApi\Options\Components\ConfigurationRow;
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

    /**
     * @var string
     */
    private $tokenId;

    /**
     * @var string
     */
    private $lastEventId;

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
        $this->configurationId = $this->_client->generateId();
        $this->tokenId = $this->_client->verifyToken()['id'];
        $lastEvent = $this->_client->listTokenEvents($this->tokenId, [
            'limit' => 1,
        ]);
        if (!empty($lastEvent)) {
            $this->lastEventId = $lastEvent[0]['id'];
        }
    }

    public function testConfigurationChange()
    {
        $config = $this->getConfiguration();
        $this->components->addConfiguration($config);

        $config->setDescription('new desc');
        $this->components->updateConfiguration($config);

        $events = $this->listEvents('storage.componentConfigurationChanged');
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
     * @param string $eventName
     * @return array
     */
    private function listEvents($eventName)
    {
        return $this->retry(function () {
            return $this->_client->listTokenEvents($this->tokenId, [
                'sinceId' => $this->lastEventId,
                'limit' => 1,
            ]);
        }, 10, $eventName);
    }

    /**
     * @param callable $apiCall
     * @param int $retries
     * @param string $eventName
     * @return array
     */
    private function retry($apiCall, $retries, $eventName)
    {
        $events = [];
        while ($retries > 0) {
            $events = $apiCall();
            if (empty($events) || $events[0]['event'] !== $eventName) {
                $retries--;
                usleep(250 * 1000);
            } else {
                break;
            }
        }
        return $events;
    }

    private function assertEvent(
        $event,
        $expectedEventName,
        $expectedEventMessage,
        $expectedObjectId,
        $expectedObjectName,
        $expectedObjectType,
        $expectedParams
    ) {
        self::assertArrayHasKey('objectName', $event);
        self::assertEquals($expectedObjectName, $event['objectName']);
        self::assertArrayHasKey('objectType', $event);
        self::assertEquals($expectedObjectType, $event['objectType']);
        self::assertArrayHasKey('objectId', $event);
        self::assertEquals($expectedObjectId, $event['objectId']);
        self::assertArrayHasKey('event', $event);
        self::assertEquals($expectedEventName, $event['event']);
        self::assertArrayHasKey('message', $event);
        self::assertEquals($expectedEventMessage, $event['message']);
        self::assertArrayHasKey('token', $event);
        self::assertEquals($this->tokenId, $event['token']['id']);
        self::assertArrayHasKey('params', $event);
        self::assertSame($expectedParams, $event['params']);
    }

    public function testConfigurationCreate()
    {
        $config = $this->getConfiguration();
        $this->components->addConfiguration($config);

        // check create event
        $events = $this->listEvents('storage.componentConfigurationCreated');
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
        $this->components->deleteConfiguration(self::COMPONENT_ID, $this->configurationId);
        $this->components->addConfiguration($this->getConfiguration());

        $events = $this->listEvents('storage.componentConfigurationRestored');
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

    public function testConfigurationDelete()
    {
        $config = $this->getConfiguration();
        $this->components->addConfiguration($config);
        // delete
        $this->components->deleteConfiguration(self::COMPONENT_ID, $this->configurationId);
        $events = $this->listEvents('storage.componentConfigurationDeleted');
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
        $this->components->deleteConfiguration(self::COMPONENT_ID, $this->configurationId);
        $events = $this->listEvents('storage.componentConfigurationPurged');
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

    public function testConfigurationRestore()
    {
        $config = $this->getConfiguration();
        $this->components->addConfiguration($config);
        $this->components->deleteConfiguration(self::COMPONENT_ID, $this->configurationId);

        $this->components->restoreComponentConfiguration(self::COMPONENT_ID, $this->configurationId);
        $events = $this->listEvents('storage.componentConfigurationRestored');

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

    public function testConfigurationVersionCopy()
    {
        $config = $this->getConfiguration();
        $this->components->addConfiguration($config);

        $newConfig = $this->components->createConfigurationFromVersion(
            self::COMPONENT_ID,
            $this->configurationId,
            1,
            'new'
        );

        $events = $this->listEvents('storage.componentConfigurationCopied');
        $this->assertEvent(
            $events[0],
            'storage.componentConfigurationCopied',
            'Created component configuration "new" (wr-db)',
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

    public function testConfigurationVersionRollback()
    {
        $config = $this->getConfiguration();
        $this->components->addConfiguration($config);
        $this->components->createConfigurationFromVersion(self::COMPONENT_ID, $this->configurationId, 1, 'v2');
        $this->components->rollbackConfiguration(self::COMPONENT_ID, $this->configurationId, 1, 'rollback');
        $events = $this->listEvents('storage.componentConfigurationRolledBack');

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

    public function testRowsChange()
    {
        $config = $this->getConfiguration();
        $this->components->addConfiguration($config);
        $rowOptions = $this->getConfigRowOptions();
        $this->components->addConfigurationRow($rowOptions);
        $rowOptions->setDescription('desc2');

        $rowResponse = $this->components->updateConfigurationRow($rowOptions);
        $events = $this->listEvents('storage.componentConfigurationRowChanged');

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

    public function testRowsCreate()
    {
        $config = $this->getConfiguration();
        $this->components->addConfiguration($config);
        $rowOptions = $this->getConfigRowOptions();

        $rowResponse = $this->components->addConfigurationRow($rowOptions);
        $events = $this->listEvents('storage.componentConfigurationRowCreated');

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

    public function testRowsDelete()
    {
        $config = $this->getConfiguration();
        $this->components->addConfiguration($config);
        $rowOptions = $this->getConfigRowOptions();
        $rowResponse = $this->components->addConfigurationRow($rowOptions);

        $this->components->deleteConfigurationRow(self::COMPONENT_ID, $this->configurationId, $rowOptions->getRowId());
        $events = $this->listEvents('storage.componentConfigurationRowDeleted');

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

    public function testRowsVersionCreate()
    {
        $config = $this->getConfiguration();
        $this->components->addConfiguration($config);
        $rowOptions = $this->getConfigRowOptions();
        $sourceRowResponse = $this->components->addConfigurationRow($rowOptions);

        $rowResponse = $this->components->createConfigurationRowFromVersion(
            self::COMPONENT_ID,
            $this->configurationId,
            $rowOptions->getRowId(),
            1
        );
        $events = $this->listEvents('storage.componentConfigurationRowCopied');
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

    public function testRowsVersionRollback()
    {
        $config = $this->getConfiguration();
        $this->components->addConfiguration($config);
        $rowOptions = $this->getConfigRowOptions();
        $rowToRollbackTo = $this->components->addConfigurationRow($rowOptions);
        $this->components->createConfigurationRowFromVersion(
            self::COMPONENT_ID,
            $this->configurationId,
            $rowOptions->getRowId(),
            1
        );

        $this->components->rollbackConfigurationRow(
            self::COMPONENT_ID,
            $this->configurationId,
            $rowOptions->getRowId(),
            1
        );
        $events = $this->listEvents('storage.componentConfigurationRowRolledBack');

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
