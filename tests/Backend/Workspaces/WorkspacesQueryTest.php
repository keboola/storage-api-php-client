<?php

namespace Keboola\Test\Backend\Workspaces;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Workspaces;
use Keboola\Test\Backend\WorkspaceConnectionTrait;
use Keboola\Test\Backend\WorkspaceCredentialsAssertTrait;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;

class WorkspacesQueryTest extends ParallelWorkspacesTestCase
{
    use WorkspaceConnectionTrait;
    use WorkspaceCredentialsAssertTrait;

    public function testWorkspaceLoadData(): void
    {
        $workspaces = new Workspaces($this->workspaceSapiClient);

        $workspace = $this->initTestWorkspace();

        $result = $workspaces->executeQuery($workspace['id'], 'SELECT 1');

        return;

        //setup test tables
        $table1_id = $this->_client->createTableAsync(
            $this->getTestBucketId(self::STAGE_IN),
            'languages',
            new CsvFile(__DIR__ . '/../../_data/languages.csv'),
        );

        $table2_id = $this->_client->createTableAsync(
            $this->getTestBucketId(self::STAGE_IN),
            'numbers',
            new CsvFile(__DIR__ . '/../../_data/numbers.csv'),
        );

        $mapping1 = ['source' => $table1_id, 'destination' => 'languagesLoaded'];
        $mapping2 = ['source' => $table2_id, 'destination' => 'numbersLoaded'];

        $input = [$mapping1, $mapping2];

        // test if job is created and listed
        $runId = $this->_client->generateRunId();
        $this->_client->setRunId($runId);
        $this->workspaceSapiClient->setRunId($runId);

        $workspaces->loadWorkspaceData($workspace['id'], ['input' => $input]);

        $afterJobs = $this->listWorkspaceJobs($workspace['id']);
        $lastJob = reset($afterJobs);
        $this->assertEquals('workspaceLoad', $lastJob['operationName']);

        // block until async events are processed, processing in order is not guaranteed but it should work most of time
        $this->createAndWaitForEvent((new \Keboola\StorageApi\Event())->setComponent('dummy')->setMessage('dummy'));

        $stats = $this->_client->getStats((new \Keboola\StorageApi\Options\StatsOptions())->setRunId($runId));

        $export = $stats['tables']['export'];
        $this->assertEquals(2, $export['totalCount']);
        $this->assertCount(2, $export['tables']);

        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        $tables = $backend->getTables();

        // check that the tables are in the workspace
        $this->assertCount(2, $tables);
        $this->assertContains($backend->toIdentifier('languagesLoaded'), $tables);
        $this->assertContains($backend->toIdentifier('numbersLoaded'), $tables);

        // check table structure and data
        $data = $backend->fetchAll('languagesLoaded', \PDO::FETCH_ASSOC);
        $this->assertCount(2, $data[0], 'there should be two columns');
        $this->assertArrayHasKey('id', $data[0]);
        $this->assertArrayHasKey('name', $data[0]);
        $this->assertArrayEqualsSorted(Client::parseCsv(file_get_contents(__DIR__ . '/../../_data/languages.csv'), true, ',', '"'), $data, 'id');

        // now we'll load another table and use the preserve parameters to check that all tables are present
        $mapping3 = ['source' => $table1_id, 'destination' => 'table3'];
        $workspaces->loadWorkspaceData($workspace['id'], ['input' => [$mapping3], 'preserve' => true]);

        $tables = $backend->getTables();

        $this->assertCount(3, $tables);
        $this->assertContains($backend->toIdentifier('table3'), $tables);
        $this->assertContains($backend->toIdentifier('languagesLoaded'), $tables);
        $this->assertContains($backend->toIdentifier('numbersLoaded'), $tables);

        // now we'll try the same load, but it should clear the workspace first (preserve is false by default)
        $workspaces->loadWorkspaceData($workspace['id'], ['input' => [$mapping3]]);

        $tables = $backend->getTables();
        $this->assertCount(1, $tables);
        $this->assertContains($backend->toIdentifier('table3'), $tables);
    }
}
