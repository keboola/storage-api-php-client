<?php

declare(strict_types=1);

namespace Backend\Snowflake;

use Generator;
use Keboola\StorageApi\Workspaces;
use Keboola\Test\Backend\WorkspaceConnectionTrait;
use Keboola\Test\Backend\WorkspaceCredentialsAssertTrait;
use Keboola\Test\Backend\Workspaces\ParallelWorkspacesTestCase;
use Keboola\Test\Utils\EventsQueryBuilder;

class WorkspacesLoginTypesTest extends ParallelWorkspacesTestCase
{
    use WorkspaceConnectionTrait;
    use WorkspaceCredentialsAssertTrait;

    /**
     * @return Generator<string, array{async:bool}>
     */
    public static function syncAsyncProvider(): Generator
    {
        yield 'sync' => [
            'async' => false,
        ];
        yield 'async' => [
            'async' => true,
        ];
    }

    public static function createWorkspaceProvider(): Generator
    {
        foreach (self::syncAsyncProvider() as $name => $syncAsync) {
            yield 'legacy login type ' . $name => [
                'loginType' => 'snowflake-legacy-service',
                'async' => $syncAsync['async'],
            ];
        }
    }

    /**
     * @dataProvider createWorkspaceProvider
     */
    public function testWorkspaceCreate(string $loginType, bool $async): void
    {
        $this->allowTestForBackendsOnly(
            [self::BACKEND_SNOWFLAKE],
            'Test only for Snowflake login types',
        );
        $this->initEvents($this->workspaceSapiClient);

        $workspaces = new Workspaces($this->workspaceSapiClient);

        foreach ($this->listTestWorkspaces($this->_client) as $workspace) {
            $workspaces->deleteWorkspace($workspace['id'], [], true);
        }

        $runId = $this->_client->generateRunId();
        $this->_client->setRunId($runId);
        $this->workspaceSapiClient->setRunId($runId);

        $workspace = $workspaces->createWorkspace([
            'backend' => 'snowflake',
            'loginType' => $loginType,
        ], $async);
        /** @var array $connection */
        $connection = $workspace['connection'];
        $this->assertSame('snowflake', $connection['backend']);
        $this->assertSame('snowflake-legacy-service', $connection['loginType']);

        // test connection is working
        $this->getDbConnectionSnowflake($connection);

        $workspaces->deleteWorkspace($workspace['id'], [], true);

        $assertCallback = function ($events) use ($runId) {
            $this->assertCount(1, $events);
            $workspaceCreatedEvent = array_pop($events);
            $this->assertSame($runId, $workspaceCreatedEvent['runId']);
            $this->assertSame('storage.workspaceCreated', $workspaceCreatedEvent['event']);
            $this->assertSame('storage', $workspaceCreatedEvent['component']);
        };

        $query = new EventsQueryBuilder();
        $query->setEvent('storage.workspaceCreated')->setRunId($runId);

        $this->assertEventWithRetries($this->workspaceSapiClient, $assertCallback, $query);

        $assertCallback = function ($events) use ($runId) {
            $this->assertCount(1, $events);
            $workspaceDeletedEvent = array_pop($events);
            $this->assertSame($runId, $workspaceDeletedEvent['runId']);
            $this->assertSame('storage.workspaceDeleted', $workspaceDeletedEvent['event']);
            $this->assertSame('storage', $workspaceDeletedEvent['component']);
        };

        $query = new EventsQueryBuilder();
        $query->setEvent('storage.workspaceDeleted')->setRunId($runId);
        $this->assertEventWithRetries($this->workspaceSapiClient, $assertCallback, $query);
        $this->assertCredentialsShouldNotWork($connection);
    }
}
