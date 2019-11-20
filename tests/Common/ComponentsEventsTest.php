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
    const CONFIGURATION_NAME = 'test';

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

    public function testConfigurationChange()
    {
        $config = $this->getConfiguration();
        $this->components->addConfiguration($config);
        $config->setDescription('new desc');
        $this->components->updateConfiguration($config);
        $events = $this->listEvents();
        self::assertEquals('storage.componentConfigurationChanged', $events[0]['event']);
        self::assertEquals('Changed component configuration "test" (wr-db)', $events[0]['message']);
    }

    public function testConfigurationCreate()
    {
        $config = $this->getConfiguration();
        $this->components->addConfiguration($config);
        // check create event
        $events = $this->listEvents();
        self::assertEquals('storage.componentConfigurationCreated', $events[0]['event']);
        self::assertEquals('Created component configuration "test" (wr-db)', $events[0]['message']);

        // check restore event on create action
        $this->components->deleteConfiguration(self::COMPONENT_ID, $this->configurationId);
        $this->components->addConfiguration($this->getConfiguration());
        $events = $this->listEvents();
        self::assertEquals('storage.componentConfigurationRestored', $events[0]['event']);
        self::assertEquals('Restored component configuration "test" (wr-db)', $events[0]['message']);
    }

    public function testConfigurationDelete()
    {
        $config = $this->getConfiguration();
        $this->components->addConfiguration($config);
        // delete
        $this->components->deleteConfiguration(self::COMPONENT_ID, $this->configurationId);
        $events = $this->listEvents();
        self::assertEquals('storage.componentConfigurationDeleted', $events[0]['event']);
        self::assertEquals('Deleted component configuration "test" (wr-db)', $events[0]['message']);

        // purge
        $this->components->deleteConfiguration(self::COMPONENT_ID, $this->configurationId);
        $events = $this->listEvents();
        self::assertEquals('storage.componentConfigurationPurged', $events[0]['event']);
        self::assertEquals('Permanently deleted component configuration "test" (wr-db)', $events[0]['message']);
    }

    /**
     * @return array
     */
    private function listEvents()
    {
        $this->waitForEventPropagation();
        return $this->_client->listTokenEvents($this->tokenId, [
            'objectId' => $this->configurationId,
        ]);
    }

    private function waitForEventPropagation()
    {
        sleep(2);
    }

    public function testConfigurationRestore()
    {
        $config = $this->getConfiguration();
        $this->components->addConfiguration($config);
        $this->components->deleteConfiguration(self::COMPONENT_ID, $this->configurationId);
        $this->components->restoreComponentConfiguration(self::COMPONENT_ID, $this->configurationId);
        $events = $this->listEvents();
        self::assertEquals('storage.componentConfigurationRestored', $events[0]['event']);
        self::assertEquals('Restored component configuration "test" (wr-db)', $events[0]['message']);
    }

    public function testConfigurationVersionCopy()
    {
        $config = $this->getConfiguration();
        $this->components->addConfiguration($config);
        $this->components->createConfigurationFromVersion(self::COMPONENT_ID, $this->configurationId, 1, 'new');
        $events = $this->listEvents();
        self::assertEquals('storage.componentConfigurationCopied', $events[0]['event']);
        self::assertEquals('Created component configuration "new" (wr-db)', $events[0]['message']);
    }

    public function testConfigurationVersionRollback()
    {
        $config = $this->getConfiguration();
        $this->components->addConfiguration($config);
        $this->components->createConfigurationFromVersion(self::COMPONENT_ID, $this->configurationId, 1, 'v2');
        $this->components->rollbackConfiguration(self::COMPONENT_ID, $this->configurationId, 1, 'rollback');
        $events = $this->listEvents();
        self::assertEquals('storage.componentConfigurationRolledBack', $events[0]['event']);
        self::assertEquals('Rolled back component configuration "test" (wr-db)', $events[0]['message']);
    }

    public function testRowsChange()
    {
        $config = $this->getConfiguration();
        $this->components->addConfiguration($config);
        $rowOptions = $this->getConfigRowOptions();
        $this->components->addConfigurationRow($rowOptions);
        $rowOptions->setDescription('desc2');

        $this->components->updateConfigurationRow($rowOptions);
        $events = $this->listEvents();
        self::assertEquals('storage.componentConfigurationRowChanged', $events[0]['event']);
        self::assertEquals('Changed component configuration row "rowName" (wr-db)', $events[0]['message']);
    }

    public function testRowsCreate()
    {
        $config = $this->getConfiguration();
        $this->components->addConfiguration($config);
        $rowOptions = $this->getConfigRowOptions();

        $this->components->addConfigurationRow($rowOptions);
        $events = $this->listEvents();
        self::assertEquals('storage.componentConfigurationRowCreated', $events[0]['event']);
        self::assertEquals('Created component configuration row "rowName" (wr-db)', $events[0]['message']);
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

    public function testRowsDelete()
    {
        $config = $this->getConfiguration();
        $this->components->addConfiguration($config);
        $rowOptions = $this->getConfigRowOptions();
        $this->components->addConfigurationRow($rowOptions);

        $this->components->deleteConfigurationRow(self::COMPONENT_ID, $this->configurationId, $rowOptions->getRowId());
        $events = $this->listEvents();
        self::assertEquals('storage.componentConfigurationRowDeleted', $events[0]['event']);
        self::assertEquals('Deleted component configuration row "rowName" (wr-db)', $events[0]['message']);
    }

    public function testRowsVersionCreate()
    {
        $config = $this->getConfiguration();
        $this->components->addConfiguration($config);
        $rowOptions = $this->getConfigRowOptions();
        $this->components->addConfigurationRow($rowOptions);

        $this->components->createConfigurationRowFromVersion(
            self::COMPONENT_ID,
            $this->configurationId,
            $rowOptions->getRowId(),
            1
        );
        $events = $this->listEvents();
        self::assertEquals('storage.componentConfigurationRowCopied', $events[0]['event']);
        self::assertEquals('Copied component configuration row "rowName" (wr-db)', $events[0]['message']);
    }

    public function testRowsVersionRollback()
    {
        $config = $this->getConfiguration();
        $this->components->addConfiguration($config);
        $rowOptions = $this->getConfigRowOptions();
        $this->components->addConfigurationRow($rowOptions);
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
        $events = $this->listEvents();
        self::assertEquals('storage.componentConfigurationRowRolledBack', $events[0]['event']);
        self::assertEquals('Rolled back component configuration row "rowName" (wr-db)', $events[0]['message']);
    }
}
