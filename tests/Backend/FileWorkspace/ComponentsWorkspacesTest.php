<?php

namespace Keboola\Test\Backend\FileWorkspace;

use Keboola\StorageApi\Components;
use Keboola\StorageApi\Options\Components\Configuration;
use Keboola\StorageApi\Options\Components\ListComponentsOptions;
use Keboola\StorageApi\Options\Components\ListConfigurationWorkspacesOptions;
use Keboola\StorageApi\Workspaces;
use Keboola\Test\Backend\FileWorkspace\Backend\Abs;
use Keboola\Test\Backend\Workspaces\DeleteWorkspacesTrait;

class ComponentsWorkspacesTest extends FileWorkspaceTestCase
{
    use DeleteWorkspacesTrait;
    public function setUp(): void
    {
        parent::setUp();
        $this->deleteAllWorkspaces();

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

    public function testWorkspaceCreate(): void
    {
        $componentId = 'wr-db';
        $configurationId = 'main-1';

        // create configuration
        $components = new Components($this->_client);
        $components->addConfiguration((new Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main')
            ->setDescription('some desc'));

        // create workspace
        $workspace = $components->createConfigurationWorkspace($componentId, $configurationId, [
            'backend' => 'abs',
        ]);
        $this->assertEquals($componentId, $workspace['component']);
        $this->assertEquals($configurationId, $workspace['configurationId']);
        $this->assertArrayHasKey('backend', $workspace['connection']);
        $this->assertArrayHasKey('container', $workspace['connection']);
        $this->assertArrayHasKey('connectionString', $workspace['connection']);

        // list workspaces
        $workspaces = new Workspaces($this->_client);

        $workspacesIds = array_map(function ($workspace) {
            return $workspace['id'];
        }, $workspaces->listWorkspaces());

        $this->assertCount(1, $workspacesIds);
        $this->assertArrayHasKey($workspace['id'], array_flip($workspacesIds));

        // create second workspace
        $workspace = $components->createConfigurationWorkspace($componentId, $configurationId, [
            'backend' => 'abs',
        ]);
        $this->assertEquals($componentId, $workspace['component']);
        $this->assertEquals($configurationId, $workspace['configurationId']);
        $this->assertArrayHasKey('backend', $workspace['connection']);
        $this->assertArrayHasKey('container', $workspace['connection']);
        $this->assertArrayHasKey('connectionString', $workspace['connection']);

        $workspacesIds = array_map(function ($workspace) {
            return $workspace['id'];
        }, $workspaces->listWorkspaces());

        $this->assertCount(2, $workspacesIds);
        $this->assertArrayHasKey($workspace['id'], array_flip($workspacesIds));

        // list configuration workspace
        $componentWorkspacesIds = array_map(function ($workspace) {
            return $workspace['id'];
        }, $components->listConfigurationWorkspaces(
            (new ListConfigurationWorkspacesOptions())
                ->setComponentId($componentId)
                ->setConfigurationId($configurationId),
        ));

        $this->assertCount(2, $componentWorkspacesIds);
        $this->assertEquals($workspacesIds, $componentWorkspacesIds);

        /** @var array $connection */
        $connection = $workspace['connection'];
        // create file
        $backend = new Abs($connection);
        $fileName = $backend->uploadTestingFile();
        $files = $backend->listFiles(null);
        $this->assertCount(1, $files);
        $this->assertEquals($files[0]->getName(), $fileName);
    }
}
