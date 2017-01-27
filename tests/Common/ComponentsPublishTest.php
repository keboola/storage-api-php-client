<?php
/**
 * Created by JetBrains PhpStorm.
 * User: martinhalamicek
 * Date: 22/05/14
 * Time: 16:38
 * To change this template use File | Settings | File Templates.
 */

namespace Keboola\Test\Common;

use Keboola\Test\StorageApiTestCase;
use Keboola\StorageApi\Options\Components\ListComponentsOptions;

class ComponentsPublishTest extends StorageApiTestCase
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

    public function testConfigurationPublish()
    {
        $componentId = 'wr-db';
        $configurationId = 'main-1';
        $configOptions = (new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId($componentId)
            ->setConfigurationId($configurationId)
            ->setName('Main')
            ->setDescription('some desc');
        $components = new \Keboola\StorageApi\Components($this->_client);
        $components->addConfiguration($configOptions);

        $configuration = $components->getConfiguration($componentId, $configurationId);
        $this->assertArrayNotHasKey('published', $configuration);
        $this->assertEquals(1, $configuration['version']);

        // Publish first version of configuration
        $configuration = $components->publishConfiguration($componentId, $configurationId, 'It works');
        $this->assertArrayHasKey('published', $configuration);
        $published = $configuration['published'];
        $this->assertArrayHasKey('date', $published);
        $this->assertArrayHasKey('token', $published);
        $this->assertArrayHasKey('description', $published);

        // Make some changes
        $configOptions->setConfiguration(['test' => 'something']);
        $components->updateConfiguration($configOptions);

        $configuration = $components->getConfiguration($componentId, $configurationId);
        $this->assertArrayNotHasKey('published', $configuration);
        $this->assertEquals(2, $configuration['version']);

        $configOptions->setConfiguration(['test' => 'something2']);
        $components->updateConfiguration($configOptions);

        $configuration = $components->getConfiguration($componentId, $configurationId);
        $this->assertArrayNotHasKey('published', $configuration);
        $this->assertEquals(3, $configuration['version']);

        // publish another version
        $components->publishConfiguration($componentId, $configurationId, 'Upgrade');
        $configuration = $components->getConfiguration($componentId, $configurationId);
        $this->assertArrayHasKey('published', $configuration);
        $this->assertEquals('Upgrade', $configuration['published']['description']);
        $this->assertEquals(3, $configuration['version']);

        // open another work version
        $configOptions->setConfiguration(['test' => 'something3']);
        $components->updateConfiguration($configOptions);

        // list versions and check the publish history
        $versions = $components->listConfigurationVersions(
            (new \Keboola\StorageApi\Options\Components\ListConfigurationVersionsOptions())
                ->setComponentId($componentId)
                ->setConfigurationId($configurationId)
        );
        $this->assertCount(3, $versions, 'one version between published versions should be removed');
        $this->assertArrayNotHasKey('published', $versions[0]);
        $this->assertArrayHasKey('published', $versions[1]);
        $this->assertArrayHasKey('published', $versions[2]);

        // get latest published version
        $configuration = $components->getConfigurationVersion($componentId, $configurationId, \Keboola\StorageApi\Options\Components\Configuration::LATEST_PUBLISHED_VERSION);
        $this->assertEquals(3, $configuration['version']);
    }

    public function testPublishAndRows()
    {
        $initialConfig = ['dfs' => 'hov'];
        $componentId = 'wr-db';
        $configurationId = 'main-1';

        $configOptions = (new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId($componentId)
            ->setConfigurationId($configurationId)
            ->setConfiguration($initialConfig)
            ->setName('Main')
            ->setDescription('some desc');
        $components = new \Keboola\StorageApi\Components($this->_client);
        $components->addConfiguration($configOptions);

        $components->addConfigurationRow(
            (new \Keboola\StorageApi\Options\Components\ConfigurationRow($configOptions))
                ->setConfiguration(['asdf' => 'dd'])
        );

        $components->publishConfiguration($componentId, $configurationId, 'one row');

        $components->addConfigurationRow(
            (new \Keboola\StorageApi\Options\Components\ConfigurationRow($configOptions))
                ->setConfiguration(['asdf' => 'second'])
        );

        $configuration = $components->getConfiguration($componentId, $configurationId);
        $this->assertArrayNotHasKey('published', $configuration, 'row add should create working version');
        $this->assertCount(2, $configuration['rows']);

        // latest published
        $configuration = $components->getConfigurationVersion($componentId, $configurationId, \Keboola\StorageApi\Options\Components\Configuration::LATEST_PUBLISHED_VERSION);
        $this->assertArrayHasKey('published', $configuration);
        $this->assertCount(1, $configuration['rows']);
    }

    public function testPublishAndRollback()
    {
        $initialConfig = ['dfs' => 'hov'];
        $componentId = 'wr-db';
        $configurationId = 'main-1';
        $configOptions = (new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId($componentId)
            ->setConfigurationId($configurationId)
            ->setConfiguration($initialConfig)
            ->setName('Main')
            ->setDescription('some desc');
        $components = new \Keboola\StorageApi\Components($this->_client);
        $components->addConfiguration($configOptions);

        // publish
        $components->publishConfiguration($componentId, $configurationId, 'init');

        // modify
        $configOptions->setConfiguration(['test' => 'something']);
        $components->updateConfiguration($configOptions);

        $configuration = $components->getConfiguration($componentId, $configurationId);
        $this->assertArrayNotHasKey('published', $configuration);

        // publish
        $components->publishConfiguration($componentId, $configurationId, 'added column');
        $configuration = $components->getConfiguration($componentId, $configurationId);
        $this->assertArrayHasKey('published', $configuration);
        $this->assertEquals(2, $configuration['version']);

        // rollback
        $components->rollbackConfiguration($componentId, $configurationId, 1);
        $configuration = $components->getConfiguration($componentId, $configurationId);
        $this->assertArrayNotHasKey('published', $configuration);
        $this->assertEquals(3, $configuration['version']);
        $this->assertEquals($initialConfig, $configuration['configuration']);

        // check versions
        $versions = $components->listConfigurationVersions(
            (new \Keboola\StorageApi\Options\Components\ListConfigurationVersionsOptions())
                ->setComponentId($componentId)
                ->setConfigurationId($configurationId)
        );
        $this->assertCount(3, $versions);
        $this->assertArrayNotHasKey('published', $versions[0], 'latest version is working version');
        $this->assertArrayHasKey('published', $versions[1], 'this is latest published version');
        $this->assertArrayHasKey('published', $versions[2], 'this is  published version');
    }

    public function testWorkingVersionsAutoDelete()
    {
        $initialConfig = ['dfs' => 'hov'];
        $componentId = 'wr-db';
        $configurationId = 'main-1';
        $configOptions = (new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId($componentId)
            ->setConfigurationId($configurationId)
            ->setConfiguration($initialConfig)
            ->setName('Main')
            ->setDescription('some desc');
        $components = new \Keboola\StorageApi\Components($this->_client);
        $components->addConfiguration($configOptions);

        // modify - add and update row few times
        $row = new \Keboola\StorageApi\Options\Components\ConfigurationRow($configOptions);
        $row->setConfiguration(['row' => 1]);
        $rowResponse = $components->addConfigurationRow($row);

        $row->setRowId($rowResponse['id']);
        $row->setConfiguration(['row' => 2]);
        $components->updateConfigurationRow($row);

        $row->setConfiguration(['row' => 3]);
        $components->updateConfigurationRow($row);

        $rowVersions = $components->listConfigurationRowVersions(
            (new \Keboola\StorageApi\Options\Components\ListConfigurationRowVersionsOptions())
                ->setComponentId($componentId)
                ->setConfigurationId($configurationId)
                ->setRowId($row->getRowId())
        );
        $this->assertCount(3, $rowVersions);

        // modify
        $configOptions->setConfiguration(['test' => '1']);
        $components->updateConfiguration($configOptions);

        $configOptions->setConfiguration(['test' => '2']);
        $components->updateConfiguration($configOptions);

        $configOptions->setConfiguration(['test' => '3']);
        $components->updateConfiguration($configOptions);

        // publish
        $components->publishConfiguration($componentId, $configurationId, 'first publish');

        // modify
        $configOptions->setConfiguration(['test' => '4']);
        $components->updateConfiguration($configOptions);

        $row->setConfiguration(['row' => 4]);
        $components->updateConfigurationRow($row);

        $row->setConfiguration(['row' => 5]);
        $components->updateConfigurationRow($row);

        $row->setConfiguration(['row' => 6]);
        $components->updateConfigurationRow($row);

        // publish
        $components->publishConfiguration($componentId, $configurationId, 'second publish');

        // modify
        $configOptions->setConfiguration(['test' => '5']);
        $components->updateConfiguration($configOptions);

        $configOptions->setConfiguration(['test' => '6']);
        $components->updateConfiguration($configOptions);

        $configuration = $components->getConfiguration($componentId, $configurationId);
        $this->assertCount(1, $configuration['rows']);
        $this->assertEquals(['row' => 6], $configuration['rows'][0]['configuration']);

        $rowVersions = $components->listConfigurationRowVersions(
            (new \Keboola\StorageApi\Options\Components\ListConfigurationRowVersionsOptions())
                ->setComponentId($componentId)
                ->setConfigurationId($configurationId)
                ->setRowId($configuration['rows'][0]['id'])
        );
        $this->assertCount(2, $rowVersions, 'Previous row versions should be deleted');

        $versions = $components->listConfigurationVersions(
            (new \Keboola\StorageApi\Options\Components\ListConfigurationVersionsOptions())
                ->setComponentId($componentId)
                ->setConfigurationId($configurationId)
        );

        // last 2 working versions and 2 published versions should be present
        $this->assertCount(4, $versions);
    }


    public function testLastWorkingVersionIsReturnedIfNotPublishedYet()
    {
        $initialConfig = ['dfs' => 'hov'];
        $componentId = 'wr-db';
        $configurationId = 'main-1';
        $configOptions = (new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId($componentId)
            ->setConfigurationId($configurationId)
            ->setConfiguration($initialConfig)
            ->setName('Main')
            ->setDescription('some desc');
        $components = new \Keboola\StorageApi\Components($this->_client);
        $components->addConfiguration($configOptions);

        $components->updateConfiguration($configOptions->setDescription('test 2'));


        $config = $components->getConfiguration($componentId, $configurationId);
        $this->assertEquals(2, $config['version']);

        $config = $components->getConfigurationVersion($componentId, $configurationId, \Keboola\StorageApi\Options\Components\Configuration::LATEST_PUBLISHED_VERSION);
        $this->assertEquals(2, $config['version']);
    }
}
