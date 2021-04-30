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
    /** @var BranchAwareClient */
    private $branchAwareClient;

    public function setUp()
    {
        parent::setUp();

        $branches = new DevBranches($this->_client);

        $branchId = null;
        foreach ($branches->listBranches() as $branch) {
            if ($branch['isDefault'] !== true) {
                $branches->deleteBranch($branch['id']);
            }
        }

        $branch = $branches->createBranch($this->getTestName());
        $this->branchAwareClient = $this->getBranchAwareDefaultClient($branch['id']);
    }

    public function testWorkspace()
    {
        $componentId = 'wr-db';
        $configurationId = 'main-1';

        // create configuration
        $components = new Components($this->branchAwareClient);
        $components->addConfiguration((new Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main')
            ->setDescription('some desc'));

        // create workspace
        $workspace = $components->createConfigurationWorkspace($componentId, $configurationId);
        $this->assertEquals($componentId, $workspace['component']);
        $this->assertEquals($configurationId, $workspace['configurationId']);
        $this->assertArrayHasKey('password', $workspace['connection']);

        // list workspaces
        $workspaces = new Workspaces($this->branchAwareClient);

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

        // load tables into workspace
        $importFile = __DIR__ . '/../../_data/languages.csv';
        $tableId = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN),
            'languages',
            new CsvFile($importFile)
        );

        $workspaces->loadWorkspaceData($workspace['id'], [
            'input' => [
                [
                    'source' => $tableId,
                    'destination' => 'languages',
                ],
            ],
        ]);

        $workspaces->cloneIntoWorkspace($workspace['id'], [
            'preserve' => true,
            'input' => [
                [
                    'source' => $tableId,
                    'destination' => 'languagesCloned',
                ],
            ],
        ]);

        // create table in workspace
        $connection = $workspace['connection'];
        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        $backend->createTable("mytable", ["amount" => ($connection['backend'] === self::BACKEND_SNOWFLAKE) ? "NUMBER" : "VARCHAR"]);

        // tables validation
        $tableNames = $backend->getTables();
        sort($tableNames);

        $this->assertCount(3, $tableNames);

        $this->assertSame('languages', $tableNames[0]);
        $this->assertSame('languagesCloned', $tableNames[1]);
        $this->assertSame('mytable', $tableNames[2]);

        // table unload
        $this->_client->createTableAsyncDirect($this->getTestBucketId(self::STAGE_IN), array(
            'name' => 'mytable',
            'dataWorkspaceId' => $workspace['id'],
            'dataTableName' => 'mytable',
        ));
    }
}
