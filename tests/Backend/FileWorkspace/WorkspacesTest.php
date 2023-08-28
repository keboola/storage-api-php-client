<?php

namespace Keboola\Test\Backend\FileWorkspace;

use Keboola\StorageApi\Workspaces;
use Keboola\Test\Backend\FileWorkspace\Backend\Abs;
use Keboola\Test\Utils\EventsQueryBuilder;
use MicrosoftAzure\Storage\Common\Exceptions\ServiceException;

class WorkspacesTest extends FileWorkspaceTestCase
{
    public function testWorkspaceCreate(): void
    {
        $this->initEvents($this->_client);
        $workspaces = new Workspaces($this->_client);

        $runId = $this->_client->generateRunId();
        $this->_client->setRunId($runId);

        $backend = $this->resolveFileWorkspaceBackend();

        $workspace = $workspaces->createWorkspace(
            [
                'backend' => $backend,
            ],
            true
        );

        $this->assertEquals($backend, $workspace['connection']['backend']);

        $connection = $workspace['connection'];
        $backend = new Abs($workspace['connection']);
        $this->assertCount(0, $backend->listFiles(null));

        $fileName = $backend->uploadTestingFile();

        $files = $backend->listFiles(null);
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

        $workspaces->deleteWorkspace($workspace['id'], [], true);

        $assertCallback = function ($events) use ($runId) {
            $this->assertCount(1, $events);
            $workspaceCreatedEvent = array_pop($events);
            $this->assertSame($runId, $workspaceCreatedEvent['runId']);
            $this->assertSame('storage.workspaceCreated', $workspaceCreatedEvent['event']);
            $this->assertSame('storage', $workspaceCreatedEvent['component']);
        };
        $query = new EventsQueryBuilder();
        $query->setEvent('storage.workspaceCreated')
            ->setTokenId($this->tokenId)
            ->setObjectId((string) $workspace['id'])
            ->setComponent('storage')
            ->setRunId($runId);
        $this->assertEventWithRetries($this->_client, $assertCallback, $query);

        $assertCallback = function ($events) use ($runId) {
            $this->assertCount(1, $events);
            $workspaceDeletedEvent = array_pop($events);
            $this->assertSame($runId, $workspaceDeletedEvent['runId']);
            $this->assertSame('storage.workspaceDeleted', $workspaceDeletedEvent['event']);
            $this->assertSame('storage', $workspaceDeletedEvent['component']);
        };

        $query = new EventsQueryBuilder();
        $query->setEvent('storage.workspaceDeleted')
            ->setTokenId($this->tokenId)
            ->setObjectId((string) $workspace['id'])
            ->setComponent('storage')
            ->setRunId($runId);
        $this->assertEventWithRetries($this->_client, $assertCallback, $query);

        $backend = new Abs($connection);
        $this->expectException(ServiceException::class);
        $this->expectExceptionMessage('The specified container does not exist');
        $backend->listFiles(null);
    }

    public function testWorkspacePasswordReset(): void
    {
        $this->initEvents($this->_client);
        $workspaces = new Workspaces($this->_client);

        $runId = $this->_client->generateRunId();
        $this->_client->setRunId($runId);

        $backend = $this->resolveFileWorkspaceBackend();

        $workspace = $workspaces->createWorkspace(
            [
                'backend' => $backend,
            ],
            true
        );

        $this->assertEquals($backend, $workspace['connection']['backend']);

        $backend = new Abs($workspace['connection']);
        $this->assertCount(0, $backend->listFiles(null));
        $fileName = $backend->uploadTestingFile();
        $files = $backend->listFiles(null);
        $this->assertCount(1, $files);
        $this->assertEquals($files[0]->getName(), $fileName);

        $runId = $this->_client->generateRunId();
        $this->_client->setRunId($runId);

        $newCredentials = $workspaces->resetWorkspacePassword($workspace['id']);
        $this->assertArrayHasKey('connectionString', $newCredentials);

        $assertCallback = function ($events) use ($runId) {
            $this->assertCount(1, $events);
            $workspaceCreatedEvent = array_pop($events);
            $this->assertSame($runId, $workspaceCreatedEvent['runId']);
            $this->assertSame('storage.workspacePasswordReset', $workspaceCreatedEvent['event']);
            $this->assertSame('storage', $workspaceCreatedEvent['component']);
        };
        $query = new EventsQueryBuilder();
        $query->setEvent('storage.workspacePasswordReset')
            ->setTokenId($this->tokenId)
            ->setObjectId((string) $workspace['id'])
            ->setComponent('storage')
            ->setRunId($runId);
        $this->assertEventWithRetries($this->_client, $assertCallback, $query);

        $workspace['connection']['connectionString'] = $newCredentials['connectionString'];
        $backend2 = new Abs($workspace['connection']);
        $files = $backend2->listFiles(null);
        $this->assertCount(1, $files);
        $this->assertEquals($files[0]->getName(), $fileName);
    }

    /**
     * @dataProvider  dropOptions
     * @param $dropOptions
     */
    public function testDropWorkspace($dropOptions): void
    {
        $workspaces = new Workspaces($this->_client);

        $backend = $this->resolveFileWorkspaceBackend();
        $workspace = $workspaces->createWorkspace(
            [
                'backend' => $backend,
            ],
            true
        );
        $backend = new Abs($workspace['connection']);
        // sync delete
        $workspaces->deleteWorkspace($workspace['id'], $dropOptions, true);
        try {
            $backend->listFiles(null);
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
            'no options - sync' => [
                [],
            ],
            'with options - async' => [
                [
                    'async' => true,
                ],
            ],
        ];
    }


    public function testCreateWorkspaceDoesNotContainConnectionString(): void
    {
        $url = 'workspaces?' . http_build_query(['async' => true]);

        $result = $this->_client->apiPostJson($url);
        // check that connectionString is not present in the response for File Workspace
        $this->assertArrayNotHasKey('connectionString', $result['connection']);
    }
}
