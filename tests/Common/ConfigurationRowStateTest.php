<?php
namespace Keboola\Test\Common;

use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Options\Components\Configuration;
use Keboola\StorageApi\Options\Components\ConfigurationRow;
use Keboola\StorageApi\Options\Components\ConfigurationRowState;
use Keboola\StorageApi\Options\Components\ListComponentConfigurationsOptions;
use Keboola\StorageApi\Options\Components\ListComponentsOptions;
use Keboola\Test\ClientProvider\ClientProvider;
use Keboola\Test\StorageApiTestCase;

class ConfigurationRowStateTest extends StorageApiTestCase
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
     * @dataProvider provideComponentsClientName
     */
    public function testAttributeExists()
    {
        $components = new \Keboola\StorageApi\Components($this->client);
        $configuration = (new \Keboola\StorageApi\Options\Components\Configuration())
                    ->setComponentId('wr-db')
                    ->setConfigurationId('main-1')
                    ->setName('Main');
        $components->addConfiguration($configuration);

        $configurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($configuration);
        $configurationRow->setRowId('main-1-1');
        $components->addConfigurationRow($configurationRow);

        $configurationResponse = $components->getConfiguration('wr-db', 'main-1');
        $this->assertArrayHasKey('state', $configurationResponse['rows'][0]);
        $this->assertEmpty($configurationResponse['rows'][0]['state']);
        $this->assertInternalType('array', $configurationResponse['rows'][0]['state']);
    }

    /**
     * @dataProvider provideComponentsClientName
     */
    public function testAttributeValueCreate()
    {
        $components = new \Keboola\StorageApi\Components($this->client);
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

    /**
     * @dataProvider provideComponentsClientName
     * @param string $clientName
     */
    public function testAttributeValueUpdate()
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
        $configurationRowResponse = $components->addConfigurationRow($configurationRow);

        $state = [
            'key' => 'val'
        ];
        $components->updateConfigurationRowState((new ConfigurationRowState($configuration))
            ->setRowId($configurationRowResponse['id'])
            ->setState($state));

        $configurationResponse = $components->getConfiguration('wr-db', 'main-1');
        $this->assertEquals($state, $configurationResponse['rows'][0]['state']);
    }

    /**
     * @dataProvider provideComponentsClientName
     * @param string $clientName
     */
    public function testVersionUnchangedAfterSettingAttribute()
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
        $configurationRowResponse = $components->addConfigurationRow($configurationRow);

        $configurationResponse = $components->getConfiguration('wr-db', 'main-1');
        $this->assertEquals(2, $configurationResponse['version']);

        $state = [
            'key' => 'val'
        ];
        $components->updateConfigurationRowState((new ConfigurationRowState($configuration))
            ->setRowId($configurationRowResponse['id'])
            ->setState($state));

        $configurationResponse = $components->getConfiguration('wr-db', 'main-1');
        $this->assertEquals(2, $configurationResponse['version']);
    }

    /**
     * @dataProvider provideComponentsClientName
     */
    public function testAttributeNotPresentInVersions()
    {
        $components = new \Keboola\StorageApi\Components($this->client);
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

        $this->assertArrayNotHasKey('state', $components->getConfigurationVersion('wr-db', 'main-1', 2)['rows'][0]);
        $this->assertArrayNotHasKey('state', $components->getConfigurationRowVersion('wr-db', 'main-1', 'main-1-1', 1));
    }

    /**
     * @dataProvider provideComponentsClientName
     * @param string $clientName
     */
    public function testRollbackPreservesState()
    {

        $components = new \Keboola\StorageApi\Components($this->client);
        $configuration = new \Keboola\StorageApi\Options\Components\Configuration();
        $configuration
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main');
        $components->addConfiguration($configuration);


        $configurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($configuration);
        $configurationRow->setRowId('main-1-1')
            ->setState(['unknown' => 'undefined']);
        $components->addConfigurationRow($configurationRow);
        
        $updateConfig = new ConfigurationRow($configuration);
        $updateConfig
            ->setRowId('main-1-1')
            ->setName('changed name');
        $components->updateConfigurationRow($updateConfig);

        $state = ['key' => 'val'];
        $components->updateConfigurationRowState((new ConfigurationRowState($configuration))
            ->setRowId('main-1-1')
            ->setState($state));

        $configurationResponse = $components->getConfiguration('wr-db', 'main-1');
        $this->assertEquals(3, $configurationResponse['version']);
        $this->assertEquals($state, $configurationResponse['rows'][0]['state']);

        $components->rollbackConfiguration('wr-db', 'main-1', 2);
        $configurationResponse = $components->getConfiguration('wr-db', 'main-1');
        $this->assertEquals(4, $configurationResponse['version']);
        $this->assertEquals($state, $configurationResponse['rows'][0]['state']);

        $components->rollbackConfiguration('wr-db', 'main-1', 3);
        $configurationResponse = $components->getConfiguration('wr-db', 'main-1');
        $this->assertEquals(5, $configurationResponse['version']);
        $this->assertEquals($state, $configurationResponse['rows'][0]['state']);
    }

    /**
     * @dataProvider provideComponentsClientName
     * @param string $clientName
     */
    public function testCopyPreservesState()
    {
        $components = new \Keboola\StorageApi\Components($this->client);
        $configuration = new \Keboola\StorageApi\Options\Components\Configuration();
        $configuration
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main');
        $components->addConfiguration($configuration);

        $configurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($configuration);
        $configurationRow->setRowId('main-1-1')
            ->setState(['unknown' => 'undefined']);
        $components->addConfigurationRow($configurationRow);

        $updateConfig = new ConfigurationRow($configuration);
        $updateConfig
            ->setRowId('main-1-1')
            ->setName('changed name');
        $components->updateConfigurationRow($updateConfig);

        $state = ['key' => 'val'];
        $components->updateConfigurationRow($updateConfig);
        $components->updateConfigurationRowState((new ConfigurationRowState($configuration))
            ->setRowId('main-1-1')
            ->setState($state));

        $newConfig = $components->createConfigurationFromVersion('wr-db', 'main-1', 3, 'main-2');
        $configurationResponse = $components->getConfiguration('wr-db', $newConfig['id']);
        $this->assertEquals($state, $configurationResponse['rows'][0]['state']);

        $newConfig = $components->createConfigurationFromVersion('wr-db', 'main-1', 2, 'main-2');
        $configurationResponse = $components->getConfiguration('wr-db', $newConfig['id']);
        $this->assertEquals($state, $configurationResponse['rows'][0]['state']);
    }

    /**
     * @dataProvider provideComponentsClientName
     * @param string $clientName
     */
    public function testRowRollbackPreservesState()
    {
        $components = new \Keboola\StorageApi\Components($this->client);
        $configuration = new \Keboola\StorageApi\Options\Components\Configuration();
        $configuration
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main');
        $components->addConfiguration($configuration);

        $configurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($configuration);
        $configurationRow->setRowId('main-1-1')
            ->setState(['unknown' => 'undefined']);
        $components->addConfigurationRow($configurationRow);

        $updateConfig = new ConfigurationRow($configuration);
        $updateConfig
            ->setRowId('main-1-1')
            ->setName('changed name');
        $components->updateConfigurationRow($updateConfig);

        $state = ['key' => 'val'];
        $components->updateConfigurationRowState((new ConfigurationRowState($configuration))
            ->setRowId('main-1-1')
            ->setState($state));
        $configurationResponse = $components->getConfiguration('wr-db', 'main-1');
        $this->assertCount(1, $configurationResponse['rows']);
        $this->assertEquals(3, $configurationResponse['version']);
        $this->assertEquals($state, $configurationResponse['rows'][0]['state']);

        $components->rollbackConfigurationRow('wr-db', 'main-1', 'main-1-1', 1);
        $configurationResponse = $components->getConfiguration('wr-db', 'main-1');
        $this->assertCount(1, $configurationResponse['rows']);
        $this->assertEquals(3, $configurationResponse['rows'][0]['version']);
        $this->assertEquals($state, $configurationResponse['rows'][0]['state']);

        $components->rollbackConfigurationRow('wr-db', 'main-1', 'main-1-1', 2);
        $configurationResponse = $components->getConfiguration('wr-db', 'main-1');
        $this->assertEquals(4, $configurationResponse['rows'][0]['version']);
        $this->assertEquals($state, $configurationResponse['rows'][0]['state']);
    }

    /**
     * @dataProvider provideComponentsClientName
     * @param string $clientName
     */
    public function testRowCopyResetsState()
    {
        $components = new \Keboola\StorageApi\Components($this->client);

        $configuration = new \Keboola\StorageApi\Options\Components\Configuration();
        $configuration
            ->setComponentId('wr-db')
            ->setConfigurationId('main-2')
            ->setName('Copy 1');
        $components->addConfiguration($configuration);

        $configuration = new \Keboola\StorageApi\Options\Components\Configuration();
        $configuration
            ->setComponentId('wr-db')
            ->setConfigurationId('main-3')
            ->setName('Copy 2');
        $components->addConfiguration($configuration);

        $configuration = new \Keboola\StorageApi\Options\Components\Configuration();
        $configuration
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main');
        $components->addConfiguration($configuration);


        $configurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($configuration);
        $configurationRow->setRowId('main-1-1')
            ->setState(['unknown' => 'undefined']);
        $components->addConfigurationRow($configurationRow);

        $updateConfig = new ConfigurationRow($configuration);
        $updateConfig
            ->setRowId('main-1-1')
            ->setName('changed name');
        $components->updateConfigurationRow($updateConfig);

        $state = ['key' => 'val'];
        $components->updateConfigurationRowState((new ConfigurationRowState($configuration))
            ->setRowId('main-1-1')
            ->setState($state));
        $components->createConfigurationRowFromVersion('wr-db', 'main-1', 'main-1-1', 1, 'main-2');
        $configurationResponse = $components->getConfiguration('wr-db', 'main-2');
        $this->assertEmpty($configurationResponse['rows'][0]['state']);

        $components->createConfigurationRowFromVersion('wr-db', 'main-1', 'main-1-1', 2, 'main-3');
        $configurationResponse = $components->getConfiguration('wr-db', 'main-3');
        $this->assertEmpty($configurationResponse['rows'][0]['state']);
    }

    /**
     * @dataProvider provideComponentsClientName
     * @param string $clientName
     */
    public function testDeletedRowRollbackPreservesState()
    {
        $components = new \Keboola\StorageApi\Components($this->client);
        $configuration = new \Keboola\StorageApi\Options\Components\Configuration();
        $configuration
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main');
        $components->addConfiguration($configuration);

        $configurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($configuration);
        $configurationRow->setRowId('main-1-1')
            ->setState(['unknown' => 'undefined']);
        $components->addConfigurationRow($configurationRow);

        $state2 = ['key' => 'val2'];
        $configurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($configuration);
        $configurationRow->setRowId('main-1-2')
            ->setState($state2);
        $components->addConfigurationRow($configurationRow);

        $updateConfig = new ConfigurationRow($configuration);
        $updateConfig
            ->setRowId('main-1-1')
            ->setName('changed name');
        $components->updateConfigurationRow($updateConfig);

        $state1 = ['key' => 'val'];
        $components->updateConfigurationRowState((new ConfigurationRowState($configuration))
            ->setRowId('main-1-1')
            ->setState($state1));

        $components->deleteConfigurationRow('wr-db', 'main-1', 'main-1-1');

        $configurationResponse = $components->getConfiguration('wr-db', 'main-1');
        $this->assertEquals(5, $configurationResponse['version']);

        $components->rollbackConfiguration('wr-db', 'main-1', 4);
        $configurationResponse = $components->getConfiguration('wr-db', 'main-1');
        $this->assertEquals(6, $configurationResponse['version']);
        $this->assertEquals($state1, $configurationResponse['rows'][0]['state']);
        $this->assertEquals($state2, $configurationResponse['rows'][1]['state']);

        $components->rollbackConfiguration('wr-db', 'main-1', 3);
        $configurationResponse = $components->getConfiguration('wr-db', 'main-1');
        $this->assertEquals(7, $configurationResponse['version']);
        $this->assertEquals($state1, $configurationResponse['rows'][0]['state']);
        $this->assertEquals($state2, $configurationResponse['rows'][1]['state']);
    }

    /**
     * @dataProvider provideComponentsClientName
     * @param string $clientName
     */
    public function testDeletedRowCopyPreservesState()
    {
        $components = new \Keboola\StorageApi\Components($this->client);
        $configuration = new \Keboola\StorageApi\Options\Components\Configuration();
        $configuration
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main');
        $components->addConfiguration($configuration);

        $configurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($configuration);
        $configurationRow->setRowId('main-1-1')
            ->setState(['unknown' => 'undefined']);
        $components->addConfigurationRow($configurationRow);

        $state2 = ['key' => 'val2'];
        $configurationRow = new \Keboola\StorageApi\Options\Components\ConfigurationRow($configuration);
        $configurationRow->setRowId('main-1-2')
            ->setState($state2);
        $components->addConfigurationRow($configurationRow);

        $updateConfig = new ConfigurationRow($configuration);
        $updateConfig
            ->setRowId('main-1-1')
            ->setName('changed name');
        $components->updateConfigurationRow($updateConfig);

        $state1 = ['key' => 'val'];
        $components->updateConfigurationRowState((new ConfigurationRowState($configuration))->setRowId('main-1-1')->setState($state1));

        $components->deleteConfigurationRow('wr-db', 'main-1', 'main-1-1');

        $configurationResponse = $components->getConfiguration('wr-db', 'main-1');
        $this->assertEquals(5, $configurationResponse['version']);

        $newConfig = $components->createConfigurationFromVersion('wr-db', 'main-1', 4, 'New Config');

        $configurationResponse = $components->getConfiguration('wr-db', $newConfig['id']);
        $this->assertEquals(1, $configurationResponse['version']);
        $this->assertEquals($state1, $configurationResponse['rows'][0]['state']);
        $this->assertEquals($state2, $configurationResponse['rows'][1]['state']);
    }
}
