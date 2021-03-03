<?php

namespace Keboola\Test\Common;

use Keboola\StorageApi\Components;
use Keboola\StorageApi\DevBranches;
use Keboola\StorageApi\Options\Components\Configuration;
use Keboola\StorageApi\Options\Components\ListComponentsOptions;
use Keboola\Test\StorageApiTestCase;

class BranchAwareClientTest extends StorageApiTestCase
{
    public function setUp()
    {
        parent::setUp();

        $components = new Components($this->_client);
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

    public function testClientWithDefaultBranch()
    {
        $devBranch = new DevBranches($this->_client);

        $defaultBranches = array_filter(
            $devBranch->listBranches(),
            function (array $branch) {
                return $branch['isDefault'] === true;
            }
        );

        $defaultBranch = reset($defaultBranches);
        $branchClient = $this->getBranchAwareDefaultClient($defaultBranch['id']);

        $branchComponents = new Components($branchClient);
        $components = new Components($this->_client);

        // create new configurations in main branch
        $componentId = 'transformation';
        $configurationId = 'main-1';
        $configurationOptions = (new Configuration())
            ->setComponentId($componentId)
            ->setConfigurationId($configurationId)
            ->setName('Main 1')
            ->setConfiguration(['test' => 'false']);

        $components->addConfiguration($configurationOptions);
        $configuration = $components->getConfiguration($componentId, $configurationId);

        $this->assertSame(
            $configuration,
            $branchComponents->getConfiguration($componentId, $configurationId)
        );

        $this->assertSame(
            $components->listComponents(),
            $branchComponents->listComponents()
        );
    }
}
