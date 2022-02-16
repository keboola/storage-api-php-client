<?php
namespace Keboola\Test\Common;

use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Options\Components\Configuration;
use Keboola\StorageApi\Options\Components\ConfigurationRow;
use Keboola\StorageApi\Options\Components\ListComponentConfigurationsOptions;
use Keboola\StorageApi\Options\Components\ListComponentsOptions;
use Keboola\Test\ClientProvider\ClientProvider;
use Keboola\Test\StorageApiTestCase;

class ConfigurationRowsSortOrderTest extends StorageApiTestCase
{
    /**
     * @var Client
     */
    private $client;

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

        $clientProvider = new ClientProvider($this);
        $this->client = $clientProvider->createClientForCurrentTest();
    }

    /**
     * @dataProvider provideComponentsClientType
     */
    public function testAttributeExists()
    {
        $components = new \Keboola\StorageApi\Components($this->client);
        $components->addConfiguration((new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main'));

        $configurationResponse = $components->getConfiguration('wr-db', 'main-1');
        $this->assertArrayHasKey('rowsSortOrder', $configurationResponse);
        $this->assertEmpty($configurationResponse['rowsSortOrder']);
        $this->assertInternalType('array', $configurationResponse['rowsSortOrder']);
    }

    /**
     * @dataProvider provideComponentsClientType
     */
    public function testAttributeValuesForOneUnsortedRow()
    {
        $components = new \Keboola\StorageApi\Components($this->client);
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
        $this->assertArrayHasKey('rowsSortOrder', $configurationResponse);
        $this->assertCount(0, $configurationResponse['rowsSortOrder']);
    }

    /**
     * @dataProvider provideComponentsClientType
     */
    public function testAttributeValuesForOneSortedRow()
    {
        $components = new \Keboola\StorageApi\Components($this->client);
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
        $this->assertArrayHasKey('rowsSortOrder', $configurationResponse);

        $updateConfig = new Configuration();
        $updateConfig
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setRowsSortOrder(['main-1-1']);
        $components->updateConfiguration($updateConfig);

        $configurationResponse = $components->getConfiguration('wr-db', 'main-1');
        $this->assertCount(1, $configurationResponse['rowsSortOrder']);
        $this->assertEquals(['main-1-1'], $configurationResponse['rowsSortOrder']);
    }

    /**
     * @dataProvider provideComponentsClientType
     */
    public function testAddRowToSorted()
    {
        $components = new \Keboola\StorageApi\Components($this->client);
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
            ->setRowsSortOrder(['main-1-1']);
        $components->updateConfiguration($updateConfig);

        $configurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($configuration);
        $configurationRow->setRowId('abcd');
        $components->addConfigurationRow($configurationRow);

        $configurationResponse = $components->getConfiguration('wr-db', 'main-1');
        $this->assertArrayHasKey('rowsSortOrder', $configurationResponse);
        $this->assertCount(2, $configurationResponse['rowsSortOrder']);
        $this->assertEquals(['main-1-1', 'abcd'], $configurationResponse['rowsSortOrder']);
        $this->assertEquals('main-1-1', $configurationResponse['rows'][0]['id']);
        $this->assertEquals('abcd', $configurationResponse['rows'][1]['id']);
    }

    /**
     * @dataProvider provideComponentsClientType
     */
    public function testAddRowToUnsorted()
    {
        $components = new \Keboola\StorageApi\Components($this->client);
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
        $configurationRow->setRowId('abcd');
        $components->addConfigurationRow($configurationRow);

        $configurationResponse = $components->getConfiguration('wr-db', 'main-1');
        $this->assertArrayHasKey('rowsSortOrder', $configurationResponse);
        $this->assertCount(0, $configurationResponse['rowsSortOrder']);
        $this->assertEquals('abcd', $configurationResponse['rows'][0]['id']);
        $this->assertEquals('main-1-1', $configurationResponse['rows'][1]['id']);
    }

    /**
     * @dataProvider provideComponentsClientType
     */
    public function testReorder()
    {
        $components = new \Keboola\StorageApi\Components($this->client);
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

    /**
     * @dataProvider provideComponentsClientType
     */
    public function testReorderNonexistingRow()
    {
        $components = new \Keboola\StorageApi\Components($this->client);
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
            $this->assertEquals(400, $e->getCode());
            $this->assertEquals('storage.components.validation', $e->getStringCode());
        }
    }

    /**
     * @dataProvider provideComponentsClientType
     */
    public function testReorderMissingRow()
    {
        $components = new \Keboola\StorageApi\Components($this->client);
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
            $this->assertEquals('validation.invalidRowSortOrder', $e->getStringCode());
        }
    }

    /**
     * @dataProvider provideComponentsClientType
     */
    public function testVersionChangeWhenRowsSortOrderIsManipulated()
    {
        $components = new \Keboola\StorageApi\Components($this->client);
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

        // running one more update without any change. The version should stay on 4
        $updateConfig = new Configuration();
        $updateConfig
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setRowsSortOrder(['main-1-2', 'main-1-1']);
        $components->updateConfiguration($updateConfig);

        $configurationResponseAfterChange = $components->getConfiguration('wr-db', 'main-1');
        $this->assertEquals(4, $configurationResponseAfterChange['version']);
    }

    /**
     * @dataProvider provideComponentsClientType
     */
    public function testVersionRollbackToUnsorted()
    {
        $components = new \Keboola\StorageApi\Components($this->client);
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
        $this->assertCount(0, $configurationResponse['rowsSortOrder']);
        $this->assertEquals('main-1-1', $configurationResponse['rows'][0]['id']);
        $this->assertEquals('main-1-2', $configurationResponse['rows'][1]['id']);
    }

    /**
     * @dataProvider provideComponentsClientType
     */
    public function testVersionRollbackToSorted()
    {
        $components = new \Keboola\StorageApi\Components($this->client);
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
            ->setRowsSortOrder(['main-1-1', 'main-1-2']);
        $components->updateConfiguration($updateConfig);

        $updateConfig = new Configuration();
        $updateConfig
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setRowsSortOrder(['main-1-2', 'main-1-1']);
        $components->updateConfiguration($updateConfig);

        $components->rollbackConfiguration('wr-db', 'main-1', 4);

        $configurationResponse = $components->getConfiguration('wr-db', 'main-1');
        $this->assertEquals(6, $configurationResponse['version']);
        $this->assertEquals(['main-1-1', 'main-1-2'], $configurationResponse['rowsSortOrder']);
        $this->assertEquals('main-1-1', $configurationResponse['rows'][0]['id']);
        $this->assertEquals('main-1-2', $configurationResponse['rows'][1]['id']);
    }

    /**
     * @dataProvider provideComponentsClientType
     */
    public function testVersionCopy()
    {
        $components = new \Keboola\StorageApi\Components($this->client);
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

        $newConfig = $components->createConfigurationFromVersion('wr-db', 'main-1', 4, 'main-2');

        $configurationResponse = $components->getConfiguration('wr-db', $newConfig["id"]);
        $this->assertEquals(['main-1-2', 'main-1-1'], $configurationResponse['rowsSortOrder']);
        $this->assertEquals('main-1-2', $configurationResponse['rows'][0]['id']);
        $this->assertEquals('main-1-1', $configurationResponse['rows'][1]['id']);

        $this->assertNotEquals('', $configurationResponse['rows'][0]['created']);
        $this->assertNotEquals(0, $configurationResponse['rows'][0]['creatorToken']['id']);
        $this->assertNotNull($configurationResponse['rows'][0]['creatorToken']['description']);

        $this->assertNotEquals('', $configurationResponse['rows'][1]['created']);
        $this->assertNotEquals(0, $configurationResponse['rows'][1]['creatorToken']['id']);
        $this->assertNotNull($configurationResponse['rows'][1]['creatorToken']['description']);
    }

    /**
     * @dataProvider provideComponentsClientType
     */
    public function testRowDeleteUnsored()
    {
        $components = new \Keboola\StorageApi\Components($this->client);
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
        $this->assertCount(0, $configurationResponse['rowsSortOrder']);
        $this->assertEquals('main-1-2', $configurationResponse['rows'][0]['id']);
        $this->assertEquals('main-1-3', $configurationResponse['rows'][1]['id']);
    }

    /**
     * @dataProvider provideComponentsClientType
     */
    public function testRowDeleteSorted()
    {
        $components = new \Keboola\StorageApi\Components($this->client);
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

        $updateConfig = new Configuration();
        $updateConfig
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setRowsSortOrder(['main-1-2', 'main-1-1', 'main-1-3']);
        $components->updateConfiguration($updateConfig);

        $components->deleteConfigurationRow('wr-db', 'main-1', 'main-1-1');
        $configurationResponse = $components->getConfiguration('wr-db', 'main-1');
        $this->assertEquals(['main-1-2', 'main-1-3'], $configurationResponse['rowsSortOrder']);
        $this->assertEquals('main-1-2', $configurationResponse['rows'][0]['id']);
        $this->assertEquals('main-1-3', $configurationResponse['rows'][1]['id']);
    }

    /**
     * @dataProvider provideComponentsClientType
     */
    public function testVersionsAttributeExists()
    {
        $components = new \Keboola\StorageApi\Components($this->client);
        $components->addConfiguration((new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main'));

        $configurationResponse = $components->getConfigurationVersion('wr-db', 'main-1', 1);
        $this->assertArrayHasKey('rowsSortOrder', $configurationResponse);
        $this->assertEmpty($configurationResponse['rowsSortOrder']);
        $this->assertInternalType('array', $configurationResponse['rowsSortOrder']);
    }

    /**
     * @dataProvider provideComponentsClientType
     */
    public function testVersionsAttributeValue()
    {
        $components = new \Keboola\StorageApi\Components($this->client);
        $configuration = new \Keboola\StorageApi\Options\Components\Configuration();
        $configuration
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main');
        $components->addConfiguration($configuration);

        $configurationResponse = $components->getConfigurationVersion('wr-db', 'main-1', 1);
        $this->assertCount(0, $configurationResponse['rowsSortOrder']);

        $configurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($configuration);
        $configurationRow->setRowId('main-1-1');
        $components->addConfigurationRow($configurationRow);

        $configurationResponse = $components->getConfigurationVersion('wr-db', 'main-1', 2);
        $this->assertCount(0, $configurationResponse['rowsSortOrder']);

        $configurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($configuration);
        $configurationRow->setRowId('main-1-2');
        $components->addConfigurationRow($configurationRow);

        $configurationResponse = $components->getConfigurationVersion('wr-db', 'main-1', 3);
        $this->assertCount(0, $configurationResponse['rowsSortOrder']);
        $this->assertEquals('main-1-1', $configurationResponse['rows'][0]['id']);
        $this->assertEquals('main-1-2', $configurationResponse['rows'][1]['id']);

        $updateConfig = new Configuration();
        $updateConfig
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setRowsSortOrder(['main-1-2', 'main-1-1']);
        $components->updateConfiguration($updateConfig);

        $configurationResponse = $components->getConfigurationVersion('wr-db', 'main-1', 4);
        $this->assertEquals(['main-1-2', 'main-1-1'], $configurationResponse['rowsSortOrder']);
        $this->assertEquals('main-1-2', $configurationResponse['rows'][0]['id']);
        $this->assertEquals('main-1-1', $configurationResponse['rows'][1]['id']);

        $components->deleteConfigurationRow('wr-db', 'main-1', 'main-1-2');

        $configurationResponse = $components->getConfigurationVersion('wr-db', 'main-1', 5);
        $this->assertEquals(['main-1-1'], $configurationResponse['rowsSortOrder']);
        $this->assertEquals('main-1-1', $configurationResponse['rows'][0]['id']);

        $configurationResponse = $components->getConfigurationVersion('wr-db', 'main-1', 4);
        $this->assertEquals(['main-1-2', 'main-1-1'], $configurationResponse['rowsSortOrder']);
        $this->assertEquals('main-1-2', $configurationResponse['rows'][0]['id']);
        $this->assertEquals('main-1-1', $configurationResponse['rows'][1]['id']);

        $configurationResponse = $components->getConfigurationVersion('wr-db', 'main-1', 3);
        $this->assertCount(0, $configurationResponse['rowsSortOrder']);
        $this->assertEquals('main-1-1', $configurationResponse['rows'][0]['id']);
        $this->assertEquals('main-1-2', $configurationResponse['rows'][1]['id']);

        $configurationResponse = $components->getConfigurationVersion('wr-db', 'main-1', 2);
        $this->assertCount(0, $configurationResponse['rowsSortOrder']);

        $configurationResponse = $components->getConfigurationVersion('wr-db', 'main-1', 1);
        $this->assertCount(0, $configurationResponse['rowsSortOrder']);
    }
}
