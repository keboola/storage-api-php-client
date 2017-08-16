<?php
/**
 * Created by JetBrains PhpStorm.
 * User: martinhalamicek
 * Date: 22/05/14
 * Time: 16:38
 * To change this template use File | Settings | File Templates.
 */

namespace Keboola\Test\Common;

use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Options\Components\Configuration;
use Keboola\StorageApi\Options\Components\ConfigurationRow;
use Keboola\StorageApi\Options\Components\ListComponentConfigurationsOptions;
use Keboola\StorageApi\Options\Components\ListComponentsOptions;
use Keboola\Test\StorageApiTestCase;

class ConfigurationRowsSortOrderTest extends StorageApiTestCase
{
    public function setUp()
    {
        parent::setUp();

        $components = new \Keboola\StorageApi\Components($this->_client);
        foreach ($components->listComponents() as $component) {
            foreach ($component['configurations'] as $configuration) {
                $components->deleteConfiguration($component['id'], $configuration['id']);
            }
        }

        // erase all deleted configurations
        foreach ($components->listComponents((new ListComponentsOptions())->setIsDeleted(true)) as $component) {
            foreach ($component['configurations'] as $configuration) {
                $components->deleteConfiguration($component['id'], $configuration['id']);
            }
        }
    }

    public function testAttributeExists()
    {
        $components = new \Keboola\StorageApi\Components($this->_client);
        $components->addConfiguration((new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main')
        );

        $configurationResponse = $components->getConfiguration('wr-db', 'main-1');
        $this->arrayHasKey('rowsSortOrder', $configurationResponse);
        $this->assertEmpty($configurationResponse['rowsSortOrder']);
        $this->assertInternalType('array', $configurationResponse['rowsSortOrder']);
    }

    public function testAttributeValuesForOneRow()
    {
        $components = new \Keboola\StorageApi\Components($this->_client);
        $configuration = new \Keboola\StorageApi\Options\Components\Configuration();
        $configuration
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main');
        $components->addConfiguration($configuration);

        $configurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($configuration);
        $configurationRow->setRowId('main-1-1');
        $components->addConfigurationRow($configurationRow);

        $configurationResponse = $components->getConfiguration('wr-db', 'main-1');
        $this->arrayHasKey('rowsSortOrder', $configurationResponse);
        $this->assertCount(1, $configurationResponse['rowsSortOrder']);
        $this->assertEquals(['main-1-1'], $configurationResponse['rowsSortOrder']);
    }

    public function testAttributeValuesForTwoRows()
    {
        $components = new \Keboola\StorageApi\Components($this->_client);
        $configuration = new \Keboola\StorageApi\Options\Components\Configuration();
        $configuration
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main');
        $components->addConfiguration($configuration);

        $configurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($configuration);
        $configurationRow->setRowId('main-1-1');
        $components->addConfigurationRow($configurationRow);

        $configurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($configuration);
        $configurationRow->setRowId('main-1-2');
        $components->addConfigurationRow($configurationRow);

        $configurationResponse = $components->getConfiguration('wr-db', 'main-1');
        $this->arrayHasKey('rowsSortOrder', $configurationResponse);
        $this->assertCount(2, $configurationResponse['rowsSortOrder']);
        $this->assertEquals(['main-1-1', 'main-2-2'], $configurationResponse['rowsSortOrder']);
        $this->assertEquals('main-1-1', $configurationResponse['rows'][0]['id']);
        $this->assertEquals('main-1-2', $configurationResponse['rows'][1]['id']);
    }

    public function testReorder()
    {
        $components = new \Keboola\StorageApi\Components($this->_client);
        $configuration = new \Keboola\StorageApi\Options\Components\Configuration();
        $configuration
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main');
        $components->addConfiguration($configuration);

        $configurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($configuration);
        $configurationRow->setRowId('main-1-1');
        $components->addConfigurationRow($configurationRow);

        $configurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($configuration);
        $configurationRow->setRowId('main-1-2');
        $components->addConfigurationRow($configurationRow);

        $updateConfig = new Configuration();
        $updateConfig
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setRowsSortOrder(['main-1-2', 'main-1-1']);
        $components->updateConfiguration($updateConfig);

        $configurationResponse = $components->getConfiguration('wr-db', 'main-1');
        $this->assertEquals(['main-1-2', 'main-1-1'], $configurationResponse['rowsSortOrder']);
        $this->assertEquals('main-1-2', $configurationResponse['rows'][0]['id']);
        $this->assertEquals('main-1-1', $configurationResponse['rows'][1]['id']);
    }

    public function testReorderNonexistingRow()
    {
        $components = new \Keboola\StorageApi\Components($this->_client);
        $configuration = new \Keboola\StorageApi\Options\Components\Configuration();
        $configuration
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main');
        $components->addConfiguration($configuration);

        $configurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($configuration);
        $configurationRow->setRowId('main-1-1');
        $components->addConfigurationRow($configurationRow);

        $updateConfig = new Configuration();
        $updateConfig
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setRowsSortOrder(['main-1-2']);
        try {
            $components->updateConfiguration($updateConfig);
            $this->fail('Invalid row ids should not be allowed.');
        } catch (\Keboola\StorageApi\ClientException $e) {
            // nebo 400?
            $this->assertEquals(404, $e->getCode());
            $this->assertEquals('validation.rowIdDoesNotExist', $e->getStringCode());
        }
    }

    public function testReorderMissingRow()
    {
        $components = new \Keboola\StorageApi\Components($this->_client);
        $configuration = new \Keboola\StorageApi\Options\Components\Configuration();
        $configuration
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main');
        $components->addConfiguration($configuration);

        $configurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($configuration);
        $configurationRow->setRowId('main-1-1');
        $components->addConfigurationRow($configurationRow);

        $configurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($configuration);
        $configurationRow->setRowId('main-1-2');
        $components->addConfigurationRow($configurationRow);

        $updateConfig = new Configuration();
        $updateConfig
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setRowsSortOrder(['main-1-1']);

        try {
            $components->updateConfiguration($updateConfig);
            $this->fail('Missing row ids should not be allowed.');
        } catch (\Keboola\StorageApi\ClientException $e) {
            // nebo 400?
            $this->assertEquals(400, $e->getCode());
            $this->assertEquals('validation.missingRowId', $e->getStringCode());
        }
    }

    public function testReorderVersions()
    {
        $components = new \Keboola\StorageApi\Components($this->_client);
        $configuration = new \Keboola\StorageApi\Options\Components\Configuration();
        $configuration
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main');
        $components->addConfiguration($configuration);

        $configurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($configuration);
        $configurationRow->setRowId('main-1-1');
        $components->addConfigurationRow($configurationRow);

        $configurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($configuration);
        $configurationRow->setRowId('main-1-2');
        $components->addConfigurationRow($configurationRow);

        $configurationResponse = $components->getConfiguration('wr-db', 'main-1');
        $this->assertEquals(3, $configurationResponse['version']);

        $updateConfig = new Configuration();
        $updateConfig
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setRowsSortOrder(['main-1-2', 'main-1-1']);
        $components->updateConfiguration($updateConfig);

        $configurationResponse = $components->getConfiguration('wr-db', 'main-1');
        $this->assertEquals(4, $configurationResponse['version']);
        $this->assertEquals(1, $configurationResponse['rows'][0]['version']);
        $this->assertEquals(1, $configurationResponse['rows'][1]['version']);
    }

    public function testVersionRollback()
    {
        $components = new \Keboola\StorageApi\Components($this->_client);
        $configuration = new \Keboola\StorageApi\Options\Components\Configuration();
        $configuration
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main');
        $components->addConfiguration($configuration);

        $configurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($configuration);
        $configurationRow->setRowId('main-1-1');
        $components->addConfigurationRow($configurationRow);

        $configurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($configuration);
        $configurationRow->setRowId('main-1-2');
        $components->addConfigurationRow($configurationRow);

        $updateConfig = new Configuration();
        $updateConfig
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setRowsSortOrder(['main-1-2', 'main-1-1']);
        $components->updateConfiguration($updateConfig);

        $components->rollbackConfiguration('wr-db', 'main-1', 3);

        $configurationResponse = $components->getConfiguration('wr-db', 'main-1');
        $this->assertEquals(5, $configurationResponse['version']);
        $this->assertEquals(['main-1-1', 'main-1-2'], $configurationResponse['rowsSortOrder']);
        $this->assertEquals('main-1-1', $configurationResponse['rows'][0]['id']);
        $this->assertEquals('main-1-2', $configurationResponse['rows'][1]['id']);
    }

    public function testVersionCopy()
    {
        $components = new \Keboola\StorageApi\Components($this->_client);
        $configuration = new \Keboola\StorageApi\Options\Components\Configuration();
        $configuration
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main');
        $components->addConfiguration($configuration);

        $configurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($configuration);
        $configurationRow->setRowId('main-1-1');
        $components->addConfigurationRow($configurationRow);

        $configurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($configuration);
        $configurationRow->setRowId('main-1-2');
        $components->addConfigurationRow($configurationRow);

        $updateConfig = new Configuration();
        $updateConfig
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setRowsSortOrder(['main-1-2', 'main-1-1']);
        $components->updateConfiguration($updateConfig);

        $components->createConfigurationFromVersion('wr-db', 'main-1', 4, 'main-2');

        $configurationResponse = $components->getConfiguration('wr-db', 'main-2');
        $this->assertEquals(['main-1-2', 'main-1-1'], $configurationResponse['rowsSortOrder']);
        $this->assertEquals('main-1-2', $configurationResponse['rows'][0]['id']);
        $this->assertEquals('main-1-1', $configurationResponse['rows'][1]['id']);
    }

    public function testRowDelete()
    {
        $components = new \Keboola\StorageApi\Components($this->_client);
        $configuration = new \Keboola\StorageApi\Options\Components\Configuration();
        $configuration
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main');
        $components->addConfiguration($configuration);

        $configurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($configuration);
        $configurationRow->setRowId('main-1-1');
        $components->addConfigurationRow($configurationRow);

        $configurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($configuration);
        $configurationRow->setRowId('main-1-2');
        $components->addConfigurationRow($configurationRow);

        $configurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($configuration);
        $configurationRow->setRowId('main-1-3');
        $components->addConfigurationRow($configurationRow);

        $components->deleteConfigurationRow('wr-db', 'main-1', 'main-1-1');
        $configurationResponse = $components->getConfiguration('wr-db', 'main-1');
        $this->assertEquals(['main-1-2', 'main-1-3'], $configurationResponse['rowsSortOrder']);
        $this->assertEquals('main-1-2', $configurationResponse['rows'][0]['id']);
        $this->assertEquals('main-1-3', $configurationResponse['rows'][1]['id']);
    }
}
