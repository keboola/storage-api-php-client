<?php

namespace Keboola\Test\Backend\Workspaces;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\BranchAwareClient;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\DevBranches;
use Keboola\StorageApi\Options\Components\Configuration;
use Keboola\StorageApi\Options\Components\ListConfigurationWorkspacesOptions;
use Keboola\StorageApi\Workspaces;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;

class BranchComponentsWorkspacesTest extends ComponentsWorkspacesTest
{
    use DeleteWorkspacesTrait;
    /** @var BranchAwareClient */
    private $branchAwareClient;

    public function setUp(): void
    {
        parent::setUp();
        $this->deleteAllWorkspaces();

        $branches = new DevBranches($this->_client);
        $this->deleteBranchesByPrefix($branches, $this->generateBranchNameForParallelTest());
        $branch = $branches->createBranch($this->generateBranchNameForParallelTest());

        $this->branchAwareClient = $this->getBranchAwareDefaultClient($branch['id']);
    }

    /**
     * @dataProvider createWorkspaceProvider
     */
    public function testWorkspace(bool $async, ?bool $roStorageAccess): void
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
        $branchComponents = new Components($this->branchAwareClient);
        $branchComponents->addConfiguration((new Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main')
            ->setDescription('some desc'));

        // create workspace
        $options = [];
        if ($roStorageAccess !== null) {
            $options['readOnlyStorageAccess'] = $roStorageAccess;
        }
        $branchWorkspace = $branchComponents->createConfigurationWorkspace($componentId, $configurationId, $options, $async);
        $this->assertEquals($componentId, $branchWorkspace['component']);
        $this->assertEquals($configurationId, $branchWorkspace['configurationId']);
        $this->assertArrayHasKey('password', $branchWorkspace['connection']);
        if ($roStorageAccess !== null) {
            $this->assertSame($roStorageAccess, $branchWorkspace['readOnlyStorageAccess']);
        }

        // list workspaces
        $branchWorkspaces = new Workspaces($this->branchAwareClient);

        $branchWorkspacesIds = array_map(function ($workspace) {
            return $workspace['id'];
        }, $branchWorkspaces->listWorkspaces());

        $this->assertCount(1, $branchWorkspacesIds);
        $this->assertArrayHasKey($branchWorkspace['id'], array_flip($branchWorkspacesIds));

        // create production configuration workspace
        $compnents = new Components($this->_client);
        $compnents->addConfiguration((new Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main')
            ->setDescription('some desc'));

        // create production  workspace
        $workspace = $compnents->createConfigurationWorkspace($componentId, $configurationId, [], $async);
        $this->assertEquals($componentId, $workspace['component']);
        $this->assertEquals($configurationId, $workspace['configurationId']);
        $this->assertArrayHasKey('password', $workspace['connection']);

        // test that production workspace is not shown in branch workspace list
        $branchWorkspacesIds = array_map(function ($branchWorkspace) {
            return $branchWorkspace['id'];
        }, $branchWorkspaces->listWorkspaces());

        // there is still only one workspace in branch, production is not shown
        $this->assertCount(1, $branchWorkspacesIds);
        // the production workspace is not in the branch workspace list
        $this->assertArrayNotHasKey($workspace['id'], array_flip($branchWorkspacesIds));

        // list production workspaces
        $workspaces = new Workspaces($this->_client);
        // test that branch workspaces are not shown in production workspace list
        $workspaceIds = array_map(function ($workspace) {
            return $workspace['id'];
        }, $workspaces->listWorkspaces());
        $this->assertArrayNotHasKey($branchWorkspace['id'], array_flip($workspaceIds));

        // create second workspace
        $branchWorkspace = $branchComponents->createConfigurationWorkspace($componentId, $configurationId, [], $async);
        $this->assertEquals($componentId, $branchWorkspace['component']);
        $this->assertEquals($configurationId, $branchWorkspace['configurationId']);
        $this->assertArrayHasKey('password', $branchWorkspace['connection']);

        $branchWorkspacesIds = array_map(function ($workspace) {
            return $workspace['id'];
        }, $branchWorkspaces->listWorkspaces());

        $this->assertCount(2, $branchWorkspacesIds);
        $this->assertArrayHasKey($branchWorkspace['id'], array_flip($branchWorkspacesIds));

        // list configuration workspace
        $componentWorkspacesIds = array_map(function ($workspace) {
            return $workspace['id'];
        }, $branchComponents->listConfigurationWorkspaces(
            (new ListConfigurationWorkspacesOptions())
                ->setComponentId($componentId)
                ->setConfigurationId($configurationId),
        ));

        $this->assertCount(2, $componentWorkspacesIds);
        $this->assertEquals($branchWorkspacesIds, $componentWorkspacesIds);

        // load tables into workspace
        $importFile = __DIR__ . '/../../_data/languages.csv';
        $tableId = $this->_client->createTableAsync(
            $this->getTestBucketId(self::STAGE_IN),
            'languages',
            new CsvFile($importFile),
        );

        /** @var int $branchWorkspaceId */
        $branchWorkspaceId = $branchWorkspace['id'];

        $branchWorkspaces->loadWorkspaceData($branchWorkspaceId, [
            'input' => [
                [
                    'source' => $tableId,
                    'destination' => 'languages',
                ],
            ],
        ]);

        $branchWorkspaces->cloneIntoWorkspace($branchWorkspaceId, [
            'preserve' => true,
            'input' => [
                [
                    'source' => $tableId,
                    'destination' => 'languagesCloned',
                ],
            ],
        ]);

        // create table in workspace
        $connection = $branchWorkspace['connection'];
        $backend = WorkspaceBackendFactory::createWorkspaceBackend($branchWorkspace);

        $backend->createTable('mytable', ['amount' => ($connection['backend'] === self::BACKEND_SNOWFLAKE) ? 'NUMBER' : 'VARCHAR']);

        // tables validation
        $tableNames = $backend->getTables();
        sort($tableNames);

        $this->assertCount(3, $tableNames);

        $this->assertSame('languages', $tableNames[0]);
        $this->assertSame('languagesCloned', $tableNames[1]);
        $this->assertSame('mytable', $tableNames[2]);

        // table unload
        $this->_client->createTableAsyncDirect($this->getTestBucketId(self::STAGE_IN), [
            'name' => 'mytable',
            'dataWorkspaceId' => $branchWorkspace['id'],
            'dataTableName' => 'mytable',
        ]);
    }
}
