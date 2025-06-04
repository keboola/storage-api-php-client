<?php

namespace Backend\Snowflake;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\Options\Components\Configuration;
use Keboola\StorageApi\Options\Components\ListComponentsOptions;
use Keboola\StorageApi\Workspaces;
use Keboola\Test\Backend\WorkspaceConnectionTrait;
use Keboola\Test\Backend\WorkspaceCredentialsAssertTrait;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;
use Keboola\Test\Backend\Workspaces\ParallelWorkspacesTestCase;

class WorkspacesReaderTest extends ParallelWorkspacesTestCase
{
    use WorkspaceConnectionTrait;
    use WorkspaceCredentialsAssertTrait;


    public function setUp(): void
    {
        parent::setUp();
        $this->initEmptyTestBucketsForParallelTests();

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

    public function testLoadToReaderAccount(): void
    {
        $componentId = 'wr-db';
        $configurationId = 'main-1';
        $defaultBranchId = $this->getDefaultBranchId($this);
        $branchClient = $this->getBranchAwareDefaultClient($defaultBranchId);
        // create configuration
        $components = new Components($branchClient);
        $components->addConfiguration((new Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('readerWS')
            ->setDescription('some desc'));

        $components = new Components($branchClient);
        $workspaces = new Workspaces($branchClient);

        $workspace = $components->createConfigurationWorkspace($componentId, $configurationId, ['useCase' => 'reader'],);

        //setup test tables
        $tableId = $this->_client->createTableAsync(
            $this->getTestBucketId(),
            'languages',
            new CsvFile(__DIR__ . '/../../_data/languages.csv'),
        );

        $workspaces->loadWorkspaceData($workspace['id'], [
            'input' => [
                [
                    'source' => $tableId,
                    'destination' => 'languages',
                ],
                [
                    'source' => $tableId,
                    'destination' => 'languages_filtered',
                    'overwrite' => false,
                    'whereColumn' => 'id',
                    'whereValues' => [1],
                    'whereOperator' => 'eq',
                ],
            ],
        ]);
        // create the connection after LOAD!! because the schema will be created by LOAD
        $db = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        $data = $db->fetchAll('languages');
        $this->assertCount(5, $data);

        $data = $db->fetchAll('languages_filtered');
        $this->assertCount(1, $data);
    }

    public function testLoadCloneToReaderAccount(): void
    {
        $componentId = 'wr-db';
        $configurationId = 'main-1';
        $defaultBranchId = $this->getDefaultBranchId($this);
        $branchClient = $this->getBranchAwareDefaultClient($defaultBranchId);
        // create configuration
        $components = new Components($branchClient);
        $components->addConfiguration((new Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('readerWS_clone')
            ->setDescription('some desc'));

        $components = new Components($branchClient);
        $workspaces = new Workspaces($branchClient);

        $workspace = $components->createConfigurationWorkspace($componentId, $configurationId, ['useCase' => 'reader']);

        //setup test tables
        $tableId = $this->_client->createTableAsync(
            $this->getTestBucketId(self::STAGE_IN),
            'languages',
            new CsvFile(__DIR__ . '/../../_data/languages.csv'),
        );

        $workspaces->cloneIntoWorkspace($workspace['id'], [
            'input' => [
                [
                    'source' => $tableId,
                    'destination' => 'languages',
                ],
                [
                    'source' => $tableId,
                    'destination' => 'langs',
                ],
            ],
        ]);
        // create the connection after LOAD!! because the schema will be created by LOAD
        $db = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        $data = $db->fetchAll('languages');
        $this->assertCount(5, $data);

        $data = $db->fetchAll('langs');
        $this->assertCount(5, $data);
    }
}
