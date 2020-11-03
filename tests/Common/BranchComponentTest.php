<?php

namespace Keboola\Test\Common;

use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\DevBranches;
use Keboola\StorageApi\Options\Components\ListComponentConfigurationsOptions;
use Keboola\StorageApi\Options\Components\ListComponentsOptions;
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

        $branch = $devBranch->createBranch($branchName);

        $branchComponents = new \Keboola\StorageApi\Components($this->getBranchAwareDefaultClient($branch['id']));
        $componentId = 'transformation';
        $branchConfigs = $branchComponents->listComponentConfigurations(
            (new ListComponentConfigurationsOptions())->setComponentId($componentId)
        );

        // empty because creating new dev branch doesn't clone configuration from main branch yet.
        $this->assertEmpty($branchConfigs);

        // create new configurations in main branch
        $components = new \Keboola\StorageApi\Components($this->_client);
        $components->addConfiguration((new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId($componentId)
            ->setConfigurationId('main-1')
            ->setName('Main 1'));
        $components->addConfiguration((new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId($componentId)
            ->setConfigurationId('main-2')
            ->setName('Main 2'));

        $configs = $components->listComponentConfigurations(
            (new ListComponentConfigurationsOptions())->setComponentId($componentId)
        );
        // two configuration was created in main branch
        $this->assertCount(2, $configs);

        $configs = $branchComponents->listComponentConfigurations(
            (new ListComponentConfigurationsOptions())->setComponentId($componentId)
        );

        // creating new configuration in main branch shouldn't create new configuration in dev branch
        $this->assertEmpty($configs);

        $mainComponentDetail = $components->getConfiguration($componentId, 'main-1');
        $this->assertNotEmpty($mainComponentDetail);

        try {
            $branchComponents->getConfiguration($componentId, 'main-1');
            $this->fail('Configuration created in main branch shouldn\'t exist in dev branch');
        } catch (ClientException $e) {
            $this->assertSame(404, $e->getCode());
            $this->assertSame('notFound', $e->getStringCode());
            $this->assertContains('Configuration main-1 not found', $e->getMessage());
        }

        $mainComponentDetail  = $components->getConfiguration($componentId, 'main-1');
        $this->assertSame('main-1', $mainComponentDetail['id']);

        //create
        $config = (new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('transformation')
            ->setConfigurationId('main-branch-1')
            ->setName('Main Branch 1')
            ->setDescription('Configuration created');

        // create new configuration in dev branch
        $branchComponents->addConfiguration($config);

        // new configuration must exist in dev branch
        $branchComponentDetail = $branchComponents->getConfiguration('transformation', 'main-branch-1');
        $this->assertEquals('Main Branch 1', $branchComponentDetail['name']);
        $this->assertEmpty($branchComponentDetail['configuration']);
        $this->assertSame('Configuration created', $branchComponentDetail['description']);
        $this->assertEquals(1, $branchComponentDetail['version']);
        $this->assertIsInt($branchComponentDetail['version']);
        $this->assertIsInt($branchComponentDetail['creatorToken']['id']);

        $configs = $branchComponents->listComponentConfigurations(
            (new ListComponentConfigurationsOptions())->setComponentId($componentId)
        );
        $this->assertCount(1, $configs);

        try {
            $components->getConfiguration('transformation', 'main-branch-1');
            $this->fail('Configuration created in dev branch shouldn\'t exist in main branch');
        } catch (ClientException $e) {
            $this->assertSame(404, $e->getCode());
            $this->assertSame('notFound', $e->getStringCode());
            $this->assertContains('Configuration main-branch-1 not found', $e->getMessage());
        }
    }
}
