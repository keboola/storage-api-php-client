<?php

namespace Keboola\Test\Backend\Synapse;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Workspaces;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;
use Keboola\Test\Backend\Workspaces\WorkspacesTestCase;

class SimpleIntCastingTest extends WorkspacesTestCase
{
    public function testCasting()
    {
        $workspaces = new Workspaces($this->_client);

        $workspace = $workspaces->createWorkspace();

        $table1_id = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN),
            'source',
            new CsvFile(__DIR__ . '/../../_data/help.keboola_source.csv')
        );

        // test if job is created and listed
        $runId = $this->_client->generateRunId();
        $this->_client->setRunId($runId);
        $workspaces->loadWorkspaceData(
            $workspace['id'],
            [
                "input" => [
                    [
                        "source" => $table1_id,
                        "destination" => "sourceLoaded",
                    ],
                ],
            ]
        );
        // block until async events are processed, processing in order is not guaranteed but it should work most of time
        $this->createAndWaitForEvent((new \Keboola\StorageApi\Event())->setComponent('dummy')->setMessage('dummy'));

        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        $tables = $backend->getTables();
        $this->assertCount(1, $tables);
        $this->assertContains($backend->toIdentifier("sourceLoaded"), $tables);

        // check table structure and data
        $data = $backend->fetchAll("sourceLoaded", \PDO::FETCH_ASSOC);
        var_export($data);
        $this->assertCount(2, $data[0], 'there should be two columns');
        $this->assertArrayHasKey('first', $data[0]);
        $this->assertArrayHasKey('second', $data[0]);
        $this->assertArrayEqualsSorted(
            Client::parseCsv(file_get_contents(__DIR__ . '/../../_data/help.keboola_source.csv'), true, ",", '"'),
            $data,
            'second'
        );
    }
}
