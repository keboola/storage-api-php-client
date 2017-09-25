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

class ConfigurationRowStateTest extends StorageApiTestCase
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
        $configuration = (new \Keboola\StorageApi\Options\Components\Configuration())
                    ->setComponentId('wr-db')
                    ->setConfigurationId('main-1')
                    ->setName('Main');
        $components->addConfiguration($configuration);

        $configurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($configuration);
        $configurationRow->setRowId('main-1-1');
        $components->addConfigurationRow($configurationRow);

        $configurationResponse = $components->getConfiguration('wr-db', 'main-1');
        $this->arrayHasKey('state', $configurationResponse['rows'][0]['state']);
        $this->assertEmpty($configurationResponse['rows'][0]['state']);
        $this->assertInternalType('array', $configurationResponse['rows'][0]['state']);
    }

    public function testAttributeValueCreate()
    {
        $components = new \Keboola\StorageApi\Components($this->_client);
        $configuration = new \Keboola\StorageApi\Options\Components\Configuration();
        $configuration
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main');
        $components->addConfiguration($configuration);

        $state = [
            'key' => 'val'
        ];

        $configurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($configuration);
        $configurationRow->setRowId('main-1-1')
            ->setState($state);
        $components->addConfigurationRow($configurationRow);

        $configurationResponse = $components->getConfiguration('wr-db', 'main-1');
        $this->assertEquals($state, $configurationResponse['rows'][0]['state']);
    }


    public function testAttributeValueUpdate()
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
        $configurationRowResponse = $components->addConfigurationRow($configurationRow);

        $state = [
            'key' => 'val'
        ];

        $updateConfig = new ConfigurationRow($configuration);
        $updateConfig
            ->setRowId($configurationRowResponse['id'])
            ->setState($state);
        $components->updateConfigurationRow($updateConfig);

        $configurationResponse = $components->getConfiguration('wr-db', 'main-1');
        $this->assertEquals($state, $configurationResponse['rows'][0]['state']);
    }

    public function testVersionUnchangedAfterSettingAttribute()
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
        $configurationRowResponse = $components->addConfigurationRow($configurationRow);

        $configurationResponse = $components->getConfiguration('wr-db', 'main-1');
        $this->assertEquals(2, $configurationResponse['version']);

        $state = [
            'key' => 'val'
        ];
        $updateConfig = new ConfigurationRow($configuration);
        $updateConfig
            ->setRowId($configurationRowResponse['id'])
            ->setState($state);
        $components->updateConfigurationRow($updateConfig);

        $configurationResponse = $components->getConfiguration('wr-db', 'main-1');
        $this->assertEquals(2, $configurationResponse['version']);
    }

    public function testAttributeNotPresentInVersions()
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

        $this->assertArrayNotHasKey('state', $components->getConfigurationVersion('wr-db', 'main-1', 2)['rows'][0]);
        $this->assertArrayNotHasKey('state', $components->getConfigurationRowVersion('wr-db', 'main-1', 'main-1-1', 1));
    }

    public function testRollbackRemovesState()
    {
        $components = new \Keboola\StorageApi\Components($this->_client);
        $configuration = new \Keboola\StorageApi\Options\Components\Configuration();
        $configuration
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main');
        $components->addConfiguration($configuration);

        $state = ['key' => 'val'];
        $configurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($configuration);
        $configurationRow->setRowId('main-1-1')
            ->setState($state);
        $components->addConfigurationRow($configurationRow);
        
        $updateConfig = new ConfigurationRow($configuration);
        $updateConfig
            ->setRowId('main-1-1')
            ->setName('changed name');
        $components->updateConfigurationRow($updateConfig);

        $configurationResponse = $components->getConfiguration('wr-db', 'main-1');
        $this->assertEquals(3, $configurationResponse['version']);
        $this->assertEquals($state, $configurationResponse['rows'][0]['state']);

        $components->rollbackConfiguration('wr-db', 'main-1', 2);

        $configurationResponse = $components->getConfiguration('wr-db', 'main-1');
        $this->assertEquals(4, $configurationResponse['version']);
        $this->assertEquals([], $configurationResponse['rows'][0]['state']);
    }

    public function testCopyRemovesState()
    {
        $components = new \Keboola\StorageApi\Components($this->_client);
        $configuration = new \Keboola\StorageApi\Options\Components\Configuration();
        $configuration
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main');
        $components->addConfiguration($configuration);

        $state = ['key' => 'val'];
        $configurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($configuration);
        $configurationRow->setRowId('main-1-1')
            ->setState($state);
        $components->addConfigurationRow($configurationRow);

        $newConfig = $components->createConfigurationFromVersion('wr-db', 'main-1', 2, 'main-2');

        $configurationResponse = $components->getConfiguration('wr-db', $newConfig['id']);
        $this->assertEquals([], $configurationResponse['rows'][0]['state']);
    }
}
