<?php

declare(strict_types=1);

namespace Backend\Snowflake;

use Generator;
use Keboola\StorageApi\ClientException;
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
            yield 'default ' . $name => [
                'loginType' => null,
                'async' => $syncAsync['async'],
                'expectedLoginType' => 'snowflake-legacy-service',
            ];
            yield 'legacy login type ' . $name => [
                'loginType' => 'snowflake-legacy-service',
                'async' => $syncAsync['async'],
                'expectedLoginType' => 'snowflake-legacy-service',
            ];
        }
    }

    /**
     * @dataProvider createWorkspaceProvider
     */
    public function testWorkspaceCreate(string|null $loginType, bool $async, string $expectedLoginType): void
    {
        $this->initEvents($this->workspaceSapiClient);

        $runId = $this->_client->generateRunId();
        $this->_client->setRunId($runId);
        $this->workspaceSapiClient->setRunId($runId);

        $workspaces = new Workspaces($this->workspaceSapiClient);
        $options = [];
        if ($loginType !== null) {
            $options['loginType'] = $loginType;
        }
        $workspace = $this->initTestWorkspace('snowflake', $options, true, $async);

        /** @var array $connection */
        $connection = $workspace['connection'];
        $this->assertSame('snowflake', $connection['backend']);
        $this->assertSame($expectedLoginType, $connection['loginType']);

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

    public function testWorkspaceWithSsoLoginType(): void
    {
        $this->initEvents($this->workspaceSapiClient);

        $runId = $this->_client->generateRunId();
        $this->_client->setRunId($runId);
        $this->workspaceSapiClient->setRunId($runId);

        $workspaces = new Workspaces($this->workspaceSapiClient);
        $options = [
            'loginType' => 'snowflake-person-sso',
            'backend' => 'snowflake',
        ];
        $this->deleteOldTestWorkspaces($this->_client);
        // we cannot use createWorkspace method as it is calling reset password directly
        // which is not allowed for SSO login type
        $jobId = $workspaces->queueCreateWorkspace($options);
        $job = $this->_client->waitForJob($jobId);
        $this->assertNotNull($job);
        /**
         * @var array{connection: array<mixed>, backend: string, loginType:string, id: int} $workspace
         */
        $workspace = $job['results'];

        /** @var array $connection */
        $connection = $workspace['connection'];
        $this->assertSame('snowflake', $connection['backend']);
        $this->assertSame('snowflake-person-sso', $connection['loginType']);
        $this->assertArrayNotHasKey('password', $connection);

        // we are not testing working connection as there is no way to connect than SSO

        try {
            // try reset password
            $workspaces->resetWorkspacePassword($workspace['id']);
            $this->fail('Password reset should not be supported for SSO login type');
        } catch (ClientException $e) {
            $this->assertSame($e->getCode(), 400);
            $this->assertSame('workspace.resetPasswordNotSupported', $e->getStringCode());
        }
    }
}
