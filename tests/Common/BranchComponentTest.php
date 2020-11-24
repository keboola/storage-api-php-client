<?php

namespace Keboola\Test\Common;

use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\DevBranches;
use Keboola\StorageApi\Options\Components\ConfigurationRow;
use Keboola\StorageApi\Options\Components\ListComponentConfigurationsOptions;
use Keboola\StorageApi\Options\Components\ListComponentsOptions;
use Keboola\StorageApi\Options\Components\ListConfigurationRowsOptions;
use Keboola\Test\StorageApiTestCase;

class BranchComponentTest extends StorageApiTestCase
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

    public function testManipulationWithComponentConfigurations()
    {
        $providedToken = $this->_client->verifyToken();
        $devBranch = new DevBranches($this->_client);
        // cleanup
        $branchesList = $devBranch->listBranches();
        $branchName = __CLASS__ . '\\' . $this->getName() . '\\' . $providedToken['id'];
        $branchesCreatedByThisTestMethod = array_filter(
            $branchesList,
            function ($branch) use ($branchName) {
                return strpos($branch['name'], $branchName) === 0;
            }
        );
        foreach ($branchesCreatedByThisTestMethod as $branch) {
            $devBranch->deleteBranch($branch['id']);
        }

        // create new configurations in main branch
        $componentId = 'transformation';
        $components = new \Keboola\StorageApi\Components($this->_client);
        $configurationOptions = (new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId($componentId)
            ->setConfigurationId('main-1')
            ->setName('Main 1');
        $components->addConfiguration($configurationOptions);
        $components->addConfigurationRow(
            (new ConfigurationRow($configurationOptions))
                ->setName('Main 1 Row 1')
                ->setRowId('main-1-row-1')
        );

        $deletedConfigurationOptions = (new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId($componentId)
            ->setConfigurationId('deleted-main')
            ->setName('Deleted Main');
        $components->addConfiguration($deletedConfigurationOptions);
        $components->addConfigurationRow(
            (new ConfigurationRow($deletedConfigurationOptions))
                ->setName('Deleted Main Row 1')
                ->setRowId('deleted-main-row-1')
        );
        // configuration exists
        $components->getConfiguration($componentId, 'deleted-main');
        $components->deleteConfiguration($componentId, 'deleted-main');
        // configuration is deleted
        try {
            $components->getConfiguration($componentId, 'deleted-main');
            $this->fail('Configuration should be deleted in the main branch');
        } catch (ClientException $e) {
            $this->assertSame(404, $e->getCode());
            $this->assertSame('notFound', $e->getStringCode());
            $this->assertContains('Configuration deleted-main not found', $e->getMessage());
        }

        // dummy branch to highlight potentially forgotten where on branch
        $devBranch->createBranch($branchName . '-dummy');

        $branch = $devBranch->createBranch($branchName);

        $branchComponents = new \Keboola\StorageApi\Components($this->getBranchAwareDefaultClient($branch['id']));

        try {
            $branchComponents->getConfiguration($componentId, 'deleted-main');
            $this->fail('Configuration deleted in the main branch shouldn\'t exist in dev branch');
        } catch (ClientException $e) {
            $this->assertSame(404, $e->getCode());
            $this->assertSame('notFound', $e->getStringCode());
            $this->assertContains('Configuration deleted-main not found', $e->getMessage());
        }

        try {
            $branchComponents->deleteConfigurationRow($componentId, 'main-1', 'main-1-row-1');
            $this->fail('Configuration row cannot be deleted in dev branch');
        } catch (ClientException $e) {
            $this->assertSame(501, $e->getCode());
            $this->assertSame('notImplemented', $e->getStringCode());
            $this->assertContains('Not implemented', $e->getMessage());
        }

        $rows = $branchComponents->listConfigurationRows((new ListConfigurationRowsOptions())
            ->setComponentId($componentId)
            ->setConfigurationId('main-1'));

        // Create new dev branch should clone all configurations and theirs config row
        // in this case is one row in main configuration
        $this->assertCount(1, $rows);

        $row = $branchComponents->getConfigurationRow(
            $componentId,
            'main-1',
            'main-1-row-1'
        );

        $this->assertEquals('main-1-row-1', $row['id']);

        $branchConfigs = $branchComponents->listComponentConfigurations(
            (new ListComponentConfigurationsOptions())->setComponentId($componentId)
        );

        // There is only the one configuration that was copied from production
        $this->assertCount(1, $branchConfigs);

        $components->addConfiguration((new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId($componentId)
            ->setConfigurationId('main-2')
            ->setName('Main 2'));

        // Add new configuration row to main branch shouldn't create new configuration row in dev branch
        $components->addConfigurationRow(
            (new ConfigurationRow($configurationOptions))
                ->setName('Main 1 Row 2')
                ->setRowId('main-1-row-2')
        );

        // Check new config rows added to main branch
        $rows = $components->listConfigurationRows((new ListConfigurationRowsOptions())
            ->setComponentId($componentId)
            ->setConfigurationId('main-1'));
        $this->assertCount(2, $rows);

        $configs = $components->listComponentConfigurations(
            (new ListComponentConfigurationsOptions())->setComponentId($componentId)
        );
        // two configuration was created in main branch
        $this->assertCount(2, $configs);

        $branchConfigs = $branchComponents->listComponentConfigurations(
            (new ListComponentConfigurationsOptions())->setComponentId($componentId)
        );

        // Creating new configuration row in main branch shouldn't create new configuration row in dev branch
        $rows = $branchComponents->listConfigurationRows((new ListConfigurationRowsOptions())
            ->setComponentId($componentId)
            ->setConfigurationId('main-1'));
        $this->assertCount(1, $rows);

        $row = $branchComponents->getConfigurationRow(
            $componentId,
            'main-1',
            'main-1-row-1'
        );

        $this->assertEquals('main-1-row-1', $row['id']);

        try {
            $branchComponents->getConfigurationRow(
                $componentId,
                'main-1',
                'main-1-row-2'
            );
            $this->fail('Configuration row created in main branch shouldn\'t exist in dev branch');
        } catch (ClientException $e) {
            $this->assertSame(404, $e->getCode());
            $this->assertSame('notFound', $e->getStringCode());
            $this->assertContains('Row main-1-row-2 not found', $e->getMessage());
        }

        // there should be the one config existing in production before creating the branch
        $this->assertCount(1, $branchConfigs);

        $mainComponentDetail = $components->getConfiguration($componentId, 'main-1');
        $this->assertNotEmpty($mainComponentDetail);

        $branchMain1Detail = $branchComponents->getConfiguration($componentId, 'main-1');
        $this->assertNotEmpty($branchMain1Detail);
        // versions are reset to 1 when copied to dev branch
        $this->assertSame(3, $mainComponentDetail['version']);
        $this->assertSame(0, $branchMain1Detail['version']);

        try {
            $branchComponents->getConfiguration($componentId, 'main-2');
            $this->fail('Configuration created in main branch after branching shouldn\'t exist in dev branch');
        } catch (ClientException $e) {
            $this->assertSame(404, $e->getCode());
            $this->assertSame('notFound', $e->getStringCode());
            $this->assertContains('Configuration main-2 not found', $e->getMessage());
        }

        $mainComponentDetail  = $components->getConfiguration($componentId, 'main-1');
        $this->assertSame('main-1', $mainComponentDetail['id']);

        // add two config rows to test rowsSortOrder
        $branchComponents->addConfigurationRow(
            (new ConfigurationRow($configurationOptions))
                ->setName('Dev 1 Row 1')
                ->setRowId('dev-1-row-1')
        );

        $configurationOptions->setRowsSortOrder(['main-1-row-1', 'dev-1-row-1']);
        $branchComponents->updateConfiguration($configurationOptions);

        $branchComponents->addConfigurationRow(
            (new ConfigurationRow($configurationOptions))
                ->setName('Dev 1 Row 3')
                ->setRowId('dev-1-row-3')
        );

        $rows = $branchComponents->listConfigurationRows((new ListConfigurationRowsOptions())
            ->setComponentId($componentId)
            ->setConfigurationId('main-1'));

        $this->assertEquals('main-1-row-1', $rows[0]['id']);
        $this->assertEquals('dev-1-row-1', $rows[1]['id']);
        $this->assertEquals('dev-1-row-3', $rows[2]['id']);
        $this->assertCount(3, $rows);

        // all version should be 1 until we implement versioning for dev branch
        $this->assertEquals(0, $rows[0]['version']);
        $this->assertEquals(1, $rows[1]['version']);
        $this->assertEquals(1, $rows[2]['version']);
        $devBranchConfiguration = $branchComponents->getConfiguration($componentId, 'main-1');
        $this->assertEquals(1, $devBranchConfiguration['version']);

        $branchComponents->updateConfigurationRow(
            (new ConfigurationRow($configurationOptions))
                ->setRowId('dev-1-row-1')
                ->setName('Renamed Dev 1 Row 1')
                ->setConfiguration('{"id":"10","stuff":"true"}')
        );

        $updatedRow = $branchComponents->getConfigurationRow(
            $componentId,
            'main-1',
            'dev-1-row-1'
        );

        $this->assertEquals('Renamed Dev 1 Row 1', $updatedRow['name']);
        $this->assertEquals('{"id":"10","stuff":"true"}', $updatedRow['configuration'][0]);
        $this->assertEquals(1, $updatedRow['version']);

        $branchComponents->updateConfigurationRow(
            (new ConfigurationRow($configurationOptions))
                ->setRowId('main-1-row-1')
                ->setName('Renamed Main 1 Row 1')
                ->setConfiguration('{"id":"10","stuff":"true"}')
        );

        $updatedRow = $branchComponents->getConfigurationRow(
            $componentId,
            'main-1',
            'main-1-row-1'
        );

        $this->assertEquals('Renamed Main 1 Row 1', $updatedRow['name']);
        $this->assertEquals('{"id":"10","stuff":"true"}', $updatedRow['configuration'][0]);
        $this->assertEquals(1, $updatedRow['version']);

        try {
            $components->getConfigurationRow(
                $componentId,
                'main-1',
                'dev-1-row-1'
            );
            $this->fail('Configuration row created in dev branch shouldn\'t exist in main branch');
        } catch (ClientException $e) {
            $this->assertSame(404, $e->getCode());
            $this->assertSame('notFound', $e->getStringCode());
            $this->assertContains('Row dev-1-row-1 not found', $e->getMessage());
        }

        //create
        $config = (new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('transformation')
            ->setConfigurationId('dev-branch-1')
            ->setName('Dev Branch 1')
            ->setDescription('Configuration created');

        // create new configuration in dev branch
        $branchComponents->addConfiguration($config);

        // new configuration must exist in dev branch
        $branchComponentDetail = $branchComponents->getConfiguration('transformation', 'dev-branch-1');
        $this->assertEquals('Dev Branch 1', $branchComponentDetail['name']);
        $this->assertEmpty($branchComponentDetail['configuration']);
        $this->assertSame('Configuration created', $branchComponentDetail['description']);
        $this->assertEquals(1, $branchComponentDetail['version']);
        $this->assertIsInt($branchComponentDetail['version']);
        $this->assertIsInt($branchComponentDetail['creatorToken']['id']);

        $configs = $branchComponents->listComponentConfigurations(
            (new ListComponentConfigurationsOptions())->setComponentId($componentId)
        );

        $this->assertCount(2, $configs);

        try {
            $components->getConfiguration('transformation', 'dev-branch-1');
            $this->fail('Configuration created in dev branch shouldn\'t exist in main branch');
        } catch (ClientException $e) {
            $this->assertSame(404, $e->getCode());
            $this->assertSame('notFound', $e->getStringCode());
            $this->assertContains('Configuration dev-branch-1 not found', $e->getMessage());
        }

        //Update
        $newName = 'neco';
        $newDesc = 'some desc';
        $configurationData = ['x' => 'y'];
        $config->setName($newName)
            ->setDescription($newDesc)
            ->setConfiguration($configurationData);
        $config->setRowsSortOrder([]);
        $branchComponents->updateConfiguration($config);

        // if update two times version is still 1
        $newName = 'neco-nove';
        $config->setName($newName)
            ->setDescription($newDesc)
            ->setConfiguration($configurationData);
        $config->setRowsSortOrder([]);
        $branchComponents->updateConfiguration($config);

        $configuration = $branchComponents->getConfiguration($config->getComponentId(), $config->getConfigurationId());

        $this->assertEquals($newName, $configuration['name']);
        $this->assertEquals($newDesc, $configuration['description']);
        $this->assertEquals($config->getConfiguration(), $configuration['configuration']);
        $this->assertEquals(1, $configuration['version']);
        $this->assertEmpty($configuration['changeDescription']);

        $config = (new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('transformation')
            ->setConfigurationId('dev-branch-1')
            ->setDescription('neco')
        ;

        $updatedConfig = $branchComponents->updateConfiguration($config);
        $this->assertEquals($newName, $updatedConfig['name'], 'Name should not be changed after description update');
        $this->assertEquals('neco', $updatedConfig['description']);
        $this->assertEquals($configurationData, $updatedConfig['configuration']);
        $this->assertEquals([], $updatedConfig['state']);
        $this->assertEmpty($updatedConfig['changeDescription']);

        $configuration = $branchComponents->getConfiguration($config->getComponentId(), $config->getConfigurationId());

        $this->assertEquals($newName, $configuration['name'], 'Name should not be changed after description update');
        $this->assertEquals('neco', $configuration['description']);
        $this->assertEquals($configurationData, $configuration['configuration']);
        $this->assertEquals([], $configuration['state']);
        $this->assertEmpty($configuration['changeDescription']);

        $state = [
            'cache' => false,
        ];

        $configState = (new \Keboola\StorageApi\Options\Components\ConfigurationState())
            ->setComponentId('transformation')
            ->setConfigurationId('dev-branch-1')
            ->setState($state)
        ;

        $updatedConfig = $branchComponents->updateConfigurationState($configState);
        $this->assertEquals($state, $updatedConfig['state']);

        $configuration = $branchComponents->getConfiguration($config->getComponentId(), $config->getConfigurationId());
        $this->assertEquals($state, $configuration['state']);

        $config = (new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('transformation')
            ->setConfigurationId('dev-branch-1')
            ->setDescription('');

        $branchComponents->updateConfiguration($config);
        $configuration = $branchComponents->getConfiguration($config->getComponentId(), $config->getConfigurationId());
        $this->assertEquals('', $configuration['description'], 'Description can be set empty');

        // List components test
        $configs = $branchComponents->listComponents();
        $this->assertCount(1, $configs);

        $branchComponents->addConfiguration((new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('branch-1')
            ->setName('Dev Branch'));
        $branchComponents->addConfiguration((new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('branch-2')
            ->setConfiguration(array('x' => 'y'))
            ->setName('Dev branch'));
        $branchComponents->addConfiguration((new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('provisioning')
            ->setConfigurationId('branch-1')
            ->setName('Dev Branch'));

        $configs = $branchComponents->listComponents();
        $this->assertCount(3, $configs);

        $configs = $branchComponents->listComponents((new ListComponentsOptions())
            ->setComponentType('writer'));

        $this->assertCount(2, $configs[0]['configurations']);
        $this->assertCount(1, $configs);

        $configuration = $configs[0]['configurations'][0];
        $this->assertArrayNotHasKey('configuration', $configuration);

        // list with configuration body
        $configs = $branchComponents->listComponents((new ListComponentsOptions())
            ->setComponentType('writer')
            ->setInclude(array('configuration')));

        $this->assertCount(2, $configs[0]['configurations']);
        $this->assertCount(1, $configs);

        $configuration = $configs[0]['configurations'][0];
        $this->assertArrayHasKey('configuration', $configuration);

        // restrict state change on configuration update
        $configuration = $branchComponents->getConfiguration($config->getComponentId(), $config->getConfigurationId());

        $config = (new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('transformation')
            ->setConfigurationId('dev-branch-1')
            ->setChangeDescription('updated state')
            ->setState([
                'cache' => true,
            ])
        ;

        try {
            $branchComponents->updateConfiguration($config);
            $this->fail('Update of configuration state should be restricted.');
        } catch (ClientException $e) {
            $this->assertEquals(400, $e->getCode());
            $this->assertEquals(
                'Using \'state\' parameter on configuration update is restricted for dev/branch context. Use direct API call.',
                $e->getMessage()
            );
        }

        $this->assertEquals(
            $configuration,
            $branchComponents->getConfiguration($config->getComponentId(), $config->getConfigurationId())
        );
    }
}
