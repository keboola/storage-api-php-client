<?php
namespace Keboola\Test\Backend\Workspaces;

use Generator;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\Options\Components\Configuration;
use Keboola\StorageApi\Options\Components\ListComponentsOptions;
use Keboola\StorageApi\Options\Components\ListConfigurationWorkspacesOptions;
use Keboola\StorageApi\Workspaces;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;

class ComponentsWorkspacesTest extends WorkspacesTestCase
{
    public function setUp(): void
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

    /**
     * @return Generator<string, array{async:bool}>
     */
    public static function createWorkspaceProvider(): Generator
    {
        yield 'sync' => [
            'async' => false,
            'roStorageAccess' => null,
        ];
        yield 'async' => [
            'async' => true,
            'roStorageAccess' => null,
        ];
        yield 'async + ro' => [
            'async' => true,
            'roStorageAccess' => true,
        ];
        yield 'async + no ro' => [
            'async' => true,
            'roStorageAccess' => false,
        ];
    }

    /**
     * @dataProvider createWorkspaceProvider
     */
    public function testWorkspaceCreate(bool $async, ?bool $roStorageAccess): void
    {
        if ($async === false) {
            $this->allowTestForBackendsOnly(
                [self::BACKEND_SNOWFLAKE],
                'Test sync actions only on Snowflake',
            );
        }
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
        $options = [];
        if ($roStorageAccess !== null) {
            $options['readOnlyStorageAccess'] = $roStorageAccess;
        }
        $workspace = $components->createConfigurationWorkspace($componentId, $configurationId, $options, $async);
        $this->assertEquals($componentId, $workspace['component']);
        $this->assertEquals($configurationId, $workspace['configurationId']);
        $this->assertArrayHasKey('password', $workspace['connection']);
        if ($roStorageAccess !== null && $workspace['connection']['backend'] === 'snowflake') {
            $this->assertSame($roStorageAccess, $workspace['readOnlyStorageAccess']);
        }

        // list workspaces
        $workspaces = new Workspaces($this->_client);

        $workspacesIds = array_map(function ($workspace) {
            return $workspace['id'];
        }, $workspaces->listWorkspaces());

        $this->assertCount(1, $workspacesIds);
        $this->assertArrayHasKey($workspace['id'], array_flip($workspacesIds));

        // create second workspace
        $workspace = $components->createConfigurationWorkspace($componentId, $configurationId, [], $async);
        $this->assertEquals($componentId, $workspace['component']);
        $this->assertEquals($configurationId, $workspace['configurationId']);
        $this->assertArrayHasKey('password', $workspace['connection']);
        $this->assertSame('service', $workspace['platformUsageType']);

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

        // create table
        $connection = $workspace['connection'];
        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        $backend->createTable('mytable', ['amount' => ($connection['backend'] === self::BACKEND_SNOWFLAKE) ? 'NUMBER' : 'VARCHAR']);

        $tableNames = $backend->getTables();
        $backend = null; // force odbc disconnect

        $this->assertArrayHasKey('mytable', array_flip($tableNames));
    }

    public function testCreateConfigurationWorkspaceDoesNotContainPassword(): void
    {
        $componentId = 'wr-db';
        $configurationId = 'main-1';
        // create configuration
        $components = new Components($this->_client);
        $components->addConfiguration((new Configuration())
            ->setComponentId($componentId)
            ->setConfigurationId($configurationId)
            ->setName('Main')
            ->setDescription('some desc'));

        $url = "components/{$componentId}/configs/{$configurationId}/workspaces?" . http_build_query(['async' => true]);

        $result = $this->_client->apiPostJson($url);
        // check that password is not present in the response
        $this->assertArrayNotHasKey('password', $result['connection']);
    }
}
