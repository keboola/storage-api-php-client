<?php

namespace Keboola\Test\Common;

use Keboola\StorageApi\BranchAwareClient;
use Keboola\StorageApi\DevBranches;
use Keboola\StorageApi\Workspaces;
use Keboola\Test\Backend\WorkspaceConnectionTrait;
use Keboola\Test\Backend\WorkspaceCredentialsAssertTrait;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;
use Keboola\Test\Backend\Workspaces\WorkspacesTest;
use Keboola\Test\Utils\EventsQueryBuilder;

class BranchWorkspacesTest extends WorkspacesTest
{
    use WorkspaceConnectionTrait;
    use WorkspaceCredentialsAssertTrait;

    private $branchId;

    public function setUp(): void
    {
        parent::setUp();

        $branches = new DevBranches($this->_client);
        $this->deleteBranchesByPrefix($branches, $this->generateBranchNameForParallelTest());

        $branch = $branches->createBranch($this->generateBranchNameForParallelTest());
        $this->branchId = $branch['id'];

        $this->workspaceSapiClient = new BranchAwareClient(
            $this->branchId,
            [
                'token' => $this->initTestToken($this->tokens),
                'url' => STORAGE_API_URL,
                'backoffMaxTries' => 1,
                'jobPollRetryDelay' => function () {
                    return 1;
                },
            ],
        );
    }

    /**
     * @dataProvider createWorkspaceProvider
     */
    public function testWorkspaceCreate(bool $async): void
    {
        $this->initEvents($this->workspaceSapiClient);

        $workspaces = new Workspaces($this->workspaceSapiClient);

        foreach ($this->listTestWorkspaces($this->_client) as $workspace) {
            $workspaces->deleteWorkspace($workspace['id']);
        }

        $runId = $this->_client->generateRunId();
        $this->workspaceSapiClient->setRunId($runId);

        $workspace = $workspaces->createWorkspace([], $async);
        $connection = $workspace['connection'];

        $tokenInfo = $this->_client->verifyToken();
        $this->assertEquals($tokenInfo['owner']['defaultBackend'], $connection['backend']);

        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace, false, 2);
        $backend->createTable('mytable', ['amount' => ($connection['backend'] === self::BACKEND_SNOWFLAKE) ? 'NUMBER' : 'VARCHAR']);

        $tableNames = $backend->getTables();
        $backend = null; // force odbc disconnect

        $this->assertArrayHasKey('mytable', array_flip($tableNames));

        // get workspace
        $workspace = $workspaces->getWorkspace($workspace['id']);
        $this->assertArrayNotHasKey('password', $workspace['connection']);

        // list workspaces
        $workspacesIds = array_map(function ($workspace) {
            return $workspace['id'];
        }, $workspaces->listWorkspaces());

        $this->assertArrayHasKey($workspace['id'], array_flip($workspacesIds));

        $workspaces->deleteWorkspace($workspace['id']);

        $assertCallback = function ($events) use ($runId) {
            $this->assertCount(1, $events);
            $workspaceCreatedEvent = array_pop($events);
            $this->assertSame($runId, $workspaceCreatedEvent['runId']);
            $this->assertSame('storage.workspaceCreated', $workspaceCreatedEvent['event']);
            $this->assertSame('storage', $workspaceCreatedEvent['component']);
            $this->assertEquals($this->branchId, $workspaceCreatedEvent['idBranch']);
        };
        $query = new EventsQueryBuilder();
        $query->setEvent('storage.workspaceCreated')
            ->setRunId($runId);
        $this->assertEventWithRetries($this->workspaceSapiClient, $assertCallback, $query);

        $assertCallback = function ($events) use ($runId) {
            $this->assertCount(1, $events);
            $workspaceDeletedEvent = array_pop($events);
            $this->assertSame($runId, $workspaceDeletedEvent['runId']);
            $this->assertSame('storage.workspaceDeleted', $workspaceDeletedEvent['event']);
            $this->assertSame('storage', $workspaceDeletedEvent['component']);
            $this->assertEquals($this->branchId, $workspaceDeletedEvent['idBranch']);
        };
        $query = new EventsQueryBuilder();
        $query->setEvent('storage.workspaceDeleted')
            ->setRunId($runId);
        $this->assertEventWithRetries($this->workspaceSapiClient, $assertCallback, $query);

        $this->assertCredentialsShouldNotWork($connection);
    }

    public function testWorkspacePasswordReset(): void
    {
        $workspaces = new Workspaces($this->workspaceSapiClient);

        $workspace = $this->initTestWorkspace();

        $connection = $workspace['connection'];

        $tokenInfo = $this->_client->verifyToken();
        $this->assertEquals($tokenInfo['owner']['defaultBackend'], $connection['backend']);

        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace, false, 2);
        $backend->dropTableIfExists('mytable');
        $backend->createTable('mytable', ['amount' => ($connection['backend'] === self::BACKEND_SNOWFLAKE) ? 'NUMBER' : 'VARCHAR']);

        $tableNames = $backend->getTables();

        $this->assertArrayHasKey('mytable', array_flip($tableNames));

        $runId = $this->_client->generateRunId();
        $this->workspaceSapiClient->setRunId($runId);

        $newCredentials = $workspaces->resetWorkspacePassword($workspace['id']);
        $this->assertArrayHasKey('password', $newCredentials);

        $this->createAndWaitForEvent((new \Keboola\StorageApi\Event())->setComponent('dummy')->setMessage('dummy'));

        $events = $this->workspaceSapiClient->listEvents([
            'runId' => $runId,
        ]);

        $workspaceCreatedEvent = array_pop($events);
        $this->assertSame($runId, $workspaceCreatedEvent['runId']);
        $this->assertSame('storage.workspacePasswordReset', $workspaceCreatedEvent['event']);
        $this->assertSame('storage', $workspaceCreatedEvent['component']);

        $this->assertEquals($this->branchId, $workspaceCreatedEvent['idBranch']);

        if ($connection['backend'] === self::BACKEND_REDSHIFT) {
            try {
                $backend->getTables();
                $this->fail('Connection session should be terminated by server');
            } catch (\PDOException $e) {
                $this->assertEquals('57P01', $e->getCode());
            }
        }

        $backend = null; // force odbc disconnect

        // old password should not work anymore
        $this->assertCredentialsShouldNotWork($connection);

        $workspace['connection']['password'] = $newCredentials['password'];
        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        $tableNames = $backend->getTables();
        $backend = null; // force odbc disconnect

        $this->assertArrayHasKey('mytable', array_flip($tableNames));
    }
}
