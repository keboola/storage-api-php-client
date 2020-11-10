<?php

namespace Keboola\Test\Backend\FileWorkspace;

use Keboola\StorageApi\Workspaces;
use Keboola\Test\Backend\FileWorkspace\Backend\Abs;
use Keboola\Test\Backend\Workspaces\WorkspacesTestCase;
use MicrosoftAzure\Storage\Common\Exceptions\ServiceException;

class WorkspacesTest extends WorkspacesTestCase
{
    public function testWorkspaceCreate()
    {
        $workspaces = new Workspaces($this->_client);

        $runId = $this->_client->generateRunId();
        $this->_client->setRunId($runId);

        $backend = $this->resolveFileWorkspaceBackend();

        $workspace = $workspaces->createWorkspace([
            'backend' => $backend,
        ]);

        $this->assertEquals($backend, $workspace['connection']['backend']);

        $connection = $workspace['connection'];
        $backend = new Abs($workspace['connection']);
        $this->assertCount(0, $backend->listFiles());

        $fileName = $backend->uploadTestingFile();

        $files = $backend->listFiles();
        $this->assertCount(1, $files);
        $this->assertEquals($files[0]->getName(), $fileName);

        // get workspace
        $workspace = $workspaces->getWorkspace($workspace['id']);
        $this->assertArrayNotHasKey('password', $workspace['connection']);

        // list workspaces
        $workspacesIds = array_map(function ($workspace) {
            return $workspace['id'];
        }, $workspaces->listWorkspaces());

        $this->assertArrayHasKey($workspace['id'], array_flip($workspacesIds));

        $workspaces->deleteWorkspace($workspace['id']);

        // block until async events are processed, processing in order is not guaranteed but it should work most of time
        $this->createAndWaitForEvent((new \Keboola\StorageApi\Event())->setComponent('dummy')->setMessage('dummy'));

        $events = $this->_client->listEvents([
            'runId' => $runId,
        ]);

        $workspaceCreatedEvent = array_pop($events);
        $this->assertSame($runId, $workspaceCreatedEvent['runId']);
        $this->assertSame('storage.workspaceCreated', $workspaceCreatedEvent['event']);
        $this->assertSame('storage', $workspaceCreatedEvent['component']);

        $workspaceDeletedEvent = array_pop($events);
        $this->assertSame($runId, $workspaceDeletedEvent['runId']);
        $this->assertSame('storage.workspaceDeleted', $workspaceDeletedEvent['event']);
        $this->assertSame('storage', $workspaceDeletedEvent['component']);
        $backend = new Abs($connection);
        $this->expectException(ServiceException::class);
        $this->expectExceptionMessage('The specified container does not exist');
        $backend->listFiles();
    }

    public function testWorkspacePasswordReset()
    {
        $workspaces = new Workspaces($this->_client);

        $runId = $this->_client->generateRunId();
        $this->_client->setRunId($runId);

        $backend = $this->resolveFileWorkspaceBackend();

        $workspace = $workspaces->createWorkspace([
            'backend' => $backend,
        ]);

        $this->assertEquals($backend, $workspace['connection']['backend']);

        $backend = new Abs($workspace['connection']);
        $this->assertCount(0, $backend->listFiles());
        $fileName = $backend->uploadTestingFile();
        $files = $backend->listFiles();
        $this->assertCount(1, $files);
        $this->assertEquals($files[0]->getName(), $fileName);

        $runId = $this->_client->generateRunId();
        $this->_client->setRunId($runId);

        $newCredentials = $workspaces->resetWorkspacePassword($workspace['id']);
        $this->assertArrayHasKey("connectionString", $newCredentials);

        $this->createAndWaitForEvent((new \Keboola\StorageApi\Event())->setComponent('dummy')->setMessage('dummy'));

        $events = $this->_client->listEvents([
            'runId' => $runId,
        ]);

        $workspaceCreatedEvent = array_pop($events);
        $this->assertSame($runId, $workspaceCreatedEvent['runId']);
        $this->assertSame('storage.workspacePasswordReset', $workspaceCreatedEvent['event']);
        $this->assertSame('storage', $workspaceCreatedEvent['component']);

        $workspace['connection']['connectionString'] = $newCredentials['connectionString'];
        $backend2 = new Abs($workspace['connection']);
        $files = $backend2->listFiles();
        $this->assertCount(1, $files);
        $this->assertEquals($files[0]->getName(), $fileName);
    }

    /**
     * @dataProvider  dropOptions
     * @param $dropOptions
     */
    public function testDropWorkspace($dropOptions)
    {
        $workspaces = new Workspaces($this->_client);

        $backend = $this->resolveFileWorkspaceBackend();
        $workspace = $workspaces->createWorkspace([
            'backend' => $backend,
        ]);
        $backend = new Abs($workspace['connection']);
        // sync delete
        $workspaces->deleteWorkspace($workspace['id'], $dropOptions);
        try {
            $backend->listFiles();
        } catch (ServiceException $e) {
            $this->assertEquals(404, $e->getCode(), $e->getMessage());
        }

        if (!empty($dropOptions['async'])) {
            $job = $this->_client->listJobs()[0];
            $this->assertEquals('workspaceDrop', $job['operationName']);
            $this->assertEquals($workspace['id'], $job['operationParams']['workspaceId']);
        }
    }

    public function dropOptions()
    {
        return [
            [
                [],
            ],
            [
                [
                    'async' => true,
                ],
            ],
        ];
    }

    /**
     * @return string
     */
    private function resolveFileWorkspaceBackend()
    {
        $tokenInfo = $this->_client->verifyToken();

        switch ($tokenInfo['owner']['fileStorageProvider']) {
            case 'azure':
                return 'abs';
            case 'aws':
            default:
                $this->markTestIncomplete(sprintf('Other file workspace provider than abs not supported'));
        }
    }
}
