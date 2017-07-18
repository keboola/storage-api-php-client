<?php
namespace Keboola\Test\Backend\Workspaces;

use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\Options\Components\Configuration;
use Keboola\StorageApi\Options\Components\ListComponentsOptions;
use Keboola\StorageApi\Options\Components\ListConfigurationWorkspacesOptions;
use Keboola\StorageApi\Workspaces;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;

class ComponentsWorkspacesTest extends WorkspacesTestCase
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

    public function testWorkspaceCreate()
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

        $component = $components->getConfiguration($componentId, $configurationId);

        $this->assertEquals('Main', $component['name']);
        $this->assertEquals('some desc', $component['description']);
        $this->assertEmpty($component['configuration']);
        $this->assertEmpty($component['changeDescription']);
        $this->assertEquals(1, $component['version']);
        $this->assertInternalType('int', $component['version']);
        $this->assertInternalType('int', $component['creatorToken']['id']);

        // create workspace
        $workspace = $components->createConfigurationWorkspace($componentId, $configurationId);
        $this->assertEquals($componentId, $workspace['component']);
        $this->assertEquals($configurationId, $workspace['configurationId']);
        $this->assertArrayHasKey('password', $workspace['connection']);

        // list workspaces
        $workspaces = new Workspaces($this->_client);

        $workspacesIds = array_map(function ($workspace) {
            return $workspace['id'];
        }, $workspaces->listWorkspaces());

        $this->assertCount(1, $workspacesIds);
        $this->assertArrayHasKey($workspace['id'], array_flip($workspacesIds));

        // create second workspace
        $workspace = $components->createConfigurationWorkspace($componentId, $configurationId);
        $this->assertEquals($componentId, $workspace['component']);
        $this->assertEquals($configurationId, $workspace['configurationId']);
        $this->assertArrayHasKey('password', $workspace['connection']);

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
            ->setConfigurationId($configurationId)
        ));

        $this->assertCount(2, $componentWorkspacesIds);
        $this->assertEquals($workspacesIds, $componentWorkspacesIds);

        // create table
        $connection = $workspace['connection'];
        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        $backend->createTable("mytable", ["amount" => ($connection['backend'] === self::BACKEND_SNOWFLAKE) ? "NUMBER" : "VARCHAR"]);

        $tableNames = $backend->getTables();
        $backend = null; // force odbc disconnect

        $this->assertArrayHasKey("mytable", array_flip($tableNames));
    }
}
