<?php

namespace Keboola\Test\Backend\Workspaces;

use Generator;
use Google\Cloud\Core\Exception\ServiceException;
use Keboola\Db\Import\Snowflake\Connection;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Workspaces;
use Keboola\Test\Backend\WorkspaceConnectionTrait;
use Keboola\Test\Backend\WorkspaceCredentialsAssertTrait;
use Keboola\Test\Backend\Workspaces\Backend\TeradataWorkspaceBackend;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;
use Keboola\Test\Utils\EventsQueryBuilder;

class WorkspacesTest extends ParallelWorkspacesTestCase
{
    use WorkspaceConnectionTrait;
    use WorkspaceCredentialsAssertTrait;

    /**
     * @return Generator<string, array{async:bool}>
     */
    public static function createWorkspaceProvider(): Generator
    {
        yield 'sync' => [
            'async' => false,
        ];
        yield 'async' => [
            'async' => true,
        ];
    }

    /**
     * @dataProvider createWorkspaceProvider
     */
    public function testWorkspaceCreate(bool $async): void
    {
        if ($async === false) {
            $this->allowTestForBackendsOnly(
                [self::BACKEND_SNOWFLAKE],
                'Test sync actions only on Snowflake',
            );
        }
        $this->initEvents($this->workspaceSapiClient);

        $workspaces = new Workspaces($this->workspaceSapiClient);

        foreach ($this->listTestWorkspaces($this->_client) as $workspace) {
            $workspaces->deleteWorkspace($workspace['id'], [], true);
        }

        $runId = $this->_client->generateRunId();
        $this->_client->setRunId($runId);
        $this->workspaceSapiClient->setRunId($runId);

        $workspace = $workspaces->createWorkspace([], $async);
        /** @var array $connection */
        $connection = $workspace['connection'];
        $this->assertArrayHasKey('region', $connection);
        $this->assertNotEmpty($connection['region']);
        $workspaceWithSnowflakeBackend = $connection['backend'] === self::BACKEND_SNOWFLAKE;
        $workspaceBackend = $connection['backend'];

        $this->assertArrayHasKey('backendSize', $workspace);
        if ($workspaceWithSnowflakeBackend) {
            $this->assertNotEmpty($connection['warehouse']);
            $this->assertSame('small', $workspace['backendSize']);
        } else {
            $this->assertNull($workspace['backendSize']);
        }

        switch ($workspaceBackend) {
            case self::BACKEND_EXASOL:
            case self::BACKEND_SNOWFLAKE: // when not specified, default is true (if feature is set = always)
            case self::BACKEND_BIGQUERY:
            case self::BACKEND_TERADATA:
                $this->assertTrue(
                    $workspace['readOnlyStorageAccess'],
                    'readOnlyStorageAccess parameter has incorrect value',
                );
                break;
            case self::BACKEND_SYNAPSE:
            case self::BACKEND_REDSHIFT:
                $this->assertFalse(
                    $workspace['readOnlyStorageAccess'],
                    'readOnlyStorageAccess parameter has incorrect value',
                );
                break;
            default:
                $this->fail(sprintf('Unexpected workspace backend "%s"', $workspaceBackend));
        }

        $tokenInfo = $this->_client->verifyToken();
        $this->assertEquals($tokenInfo['owner']['defaultBackend'], $connection['backend']);

        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        if ($workspaceWithSnowflakeBackend) {
            $db = $backend->getDb();
            assert($db instanceof Connection);
            $grants = $db->fetchAll(sprintf('SHOW GRANTS TO ROLE "%s"', $connection['user']));
            $grantsNames = array_map(function ($grant) {
                return $grant['privilege'];
            }, $grants);
            $this->assertNotContains('CREATE STREAMLIT', $grantsNames);
            $db = null; // force odbc disconnect
        }
        $backend->createTable('mytable', ['amount' => $this->getColumnAmountType($connection['backend'])]);

        $tableNames = $backend->getTables();
        $backend = null; // force odbc disconnect

        $this->assertArrayHasKey('mytable', array_flip($tableNames));

        // get workspace
        $workspace = $workspaces->getWorkspace($workspace['id']);
        $this->assertArrayNotHasKey('password', $workspace['connection']);
        $this->assertArrayHasKey('region', $workspace['connection']);
        $this->assertNotEmpty($workspace['connection']['region']);
        if ($workspaceWithSnowflakeBackend) {
            $this->assertNotEmpty($workspace['connection']['warehouse']);
        }

        // list workspaces
        $testWorkspaceInfo = null;
        foreach ($workspaces->listWorkspaces() as $workspaceInfo) {
            if ($workspaceInfo['id'] === $workspace['id']) {
                $testWorkspaceInfo = $workspaceInfo;
            }

            $this->assertArrayHasKey('region', $workspaceInfo['connection']);
            $this->assertNotEmpty($workspaceInfo['connection']['region']);
        }

        $this->assertNotNull($testWorkspaceInfo);
        $this->assertSame($workspace, $testWorkspaceInfo);

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

    public function testQueueWorkspaceCreate(): void
    {
        $async = true;
        $this->initEvents($this->workspaceSapiClient);

        $workspaces = new Workspaces($this->workspaceSapiClient);

        foreach ($this->listTestWorkspaces($this->_client) as $workspace) {
            $workspaces->deleteWorkspace($workspace['id'], [], true);
        }

        $runId = $this->_client->generateRunId();
        $this->_client->setRunId($runId);
        $this->workspaceSapiClient->setRunId($runId);

        $jobId = $workspaces->queueCreateWorkspace([]);

        $job = $this->_client->waitForJob($jobId);
        $workspace = $job['results'];
        $resetPasswordResponse = $workspaces->resetWorkspacePassword($workspace['id']);
        $workspace = Workspaces::addCredentialsToWorkspaceResponse($workspace, $resetPasswordResponse);

        /** @var array $connection */
        $connection = $workspace['connection'];
        $this->assertArrayHasKey('region', $connection);
        $this->assertNotEmpty($connection['region']);
        $workspaceWithSnowflakeBackend = $connection['backend'] === self::BACKEND_SNOWFLAKE;
        $workspaceBackend = $connection['backend'];

        $this->assertArrayHasKey('backendSize', $workspace);
        if ($workspaceWithSnowflakeBackend) {
            $this->assertNotEmpty($connection['warehouse']);
            $this->assertSame('small', $workspace['backendSize']);
        } else {
            $this->assertNull($workspace['backendSize']);
        }

        switch ($workspaceBackend) {
            case self::BACKEND_EXASOL:
            case self::BACKEND_SNOWFLAKE: // when not specified, default is true (if feature is set = always)
            case self::BACKEND_BIGQUERY:
            case self::BACKEND_TERADATA:
                $this->assertTrue(
                    $workspace['readOnlyStorageAccess'],
                    'readOnlyStorageAccess parameter has incorrect value',
                );
                break;
            case self::BACKEND_SYNAPSE:
            case self::BACKEND_REDSHIFT:
                $this->assertFalse(
                    $workspace['readOnlyStorageAccess'],
                    'readOnlyStorageAccess parameter has incorrect value',
                );
                break;
            default:
                $this->fail(sprintf('Unexpected workspace backend "%s"', $workspaceBackend));
        }

        $tokenInfo = $this->_client->verifyToken();
        $this->assertEquals($tokenInfo['owner']['defaultBackend'], $connection['backend']);

        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        if ($workspaceWithSnowflakeBackend) {
            $db = $backend->getDb();
            assert($db instanceof Connection);
            $grants = $db->fetchAll(sprintf('SHOW GRANTS TO ROLE "%s"', $connection['user']));
            $grantsNames = array_map(function ($grant) {
                return $grant['privilege'];
            }, $grants);
            $this->assertNotContains('CREATE STREAMLIT', $grantsNames);
            $db = null; // force odbc disconnect
        }
        $backend->createTable('mytable', ['amount' => $this->getColumnAmountType($connection['backend'])]);

        $tableNames = $backend->getTables();
        $backend = null; // force odbc disconnect

        $this->assertArrayHasKey('mytable', array_flip($tableNames));

        $workspaces->deleteWorkspace($workspace['id'], [], true);
    }

    public function testWorkspacePasswordReset(): void
    {
        $workspaces = new Workspaces($this->workspaceSapiClient);

        $workspace = $this->initTestWorkspace();

        $connection = $workspace['connection'];

        $tokenInfo = $this->_client->verifyToken();
        $this->assertEquals($tokenInfo['owner']['defaultBackend'], $connection['backend']);

        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);
        $backend->dropTableIfExists('mytable');

        $backend->createTable('mytable', ['amount' => $this->getColumnAmountType($connection['backend'])]);

        $tableNames = $backend->getTables();

        $this->assertArrayHasKey('mytable', array_flip($tableNames));

        $runId = $this->_client->generateRunId();
        $this->_client->setRunId($runId);
        $this->workspaceSapiClient->setRunId($runId);

        $newCredentials = $workspaces->resetWorkspacePassword($workspace['id']);
        if ($this->getDefaultBackend($this->workspaceSapiClient) === self::BACKEND_BIGQUERY) {
            $this->assertArrayHasKey('credentials', $newCredentials);
        } else {
            $this->assertArrayHasKey('password', $newCredentials);
        }

        $this->createAndWaitForEvent((new \Keboola\StorageApi\Event())->setComponent('dummy')->setMessage('dummy'));

        $events = $this->_client->listEvents([
            'runId' => $runId,
        ]);

        $workspaceCreatedEvent = array_pop($events);
        $this->assertSame($runId, $workspaceCreatedEvent['runId']);
        $this->assertSame('storage.workspacePasswordReset', $workspaceCreatedEvent['event']);
        $this->assertSame('storage', $workspaceCreatedEvent['component']);

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

        if ($this->getDefaultBackend($this->workspaceSapiClient) === self::BACKEND_BIGQUERY) {
            $workspace['connection']['credentials'] = $newCredentials['credentials'];
        } else {
            $workspace['connection']['password'] = $newCredentials['password'];
        }
        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        $tableNames = $backend->getTables();
        $backend = null; // force odbc disconnect

        $this->assertArrayHasKey('mytable', array_flip($tableNames));
    }

    /**
     * @dataProvider  dropOptions
     */
    public function testDropWorkspace(array $dropOptions, bool $async): void
    {
        if ($async === false) {
            $this->allowTestForBackendsOnly(
                [self::BACKEND_SNOWFLAKE],
                'Test sync actions only on Snowflake',
            );
        }
        $workspaces = new Workspaces($this->workspaceSapiClient);

        foreach ($this->listTestWorkspaces($this->_client) as $workspace) {
            $workspaces->deleteWorkspace($workspace['id'], [], true);
        }

        $runId = $this->_client->generateRunId();
        $this->workspaceSapiClient->setRunId($runId);

        $workspace = $workspaces->createWorkspace([], true);
        $connection = $workspace['connection'];

        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        $backend->createTable('mytable', ['amount' => $this->getColumnAmountType($connection['backend'])]);

        if ($backend instanceof TeradataWorkspaceBackend) {
            // Teradata: cannot drop workspace if user is logged in
            $backend->disconnect();
        }

        // sync delete
        $workspaces->deleteWorkspace($workspace['id'], $dropOptions, $async);

        try {
            $backend->countRows('mytable');
            $this->fail('workspace no longer exists. connection should be dead.');
        } catch (ServiceException $e) { // catch bigquery connection exception
            $this->assertStringContainsString('Request had invalid authentication credentials. ', $e->getMessage());
            $this->assertSame(401, $e->getCode());
        } catch (\PDOException $e) { // catch redshift connection exception
            $this->assertEquals('57P01', $e->getCode());
        } catch (\Doctrine\DBAL\Exception $e) {
            switch ($connection['backend']) {
                case self::BACKEND_SYNAPSE:
                    $this->assertEquals(110813, $e->getCode(), $e->getMessage());
                    break;
                case self::BACKEND_TERADATA:
                    $this->assertContains(
                        $e->getCode(),
                        [0, 2],
                        sprintf(
                            'Unexpected error message from Teradata code: "%s" message: "%s".',
                            $e->getCode(),
                            $e->getMessage(),
                        ),
                    );
                    break;
                case self::BACKEND_EXASOL:
                    $this->assertEquals(0, $e->getCode(), $e->getMessage());
                    break;
                case self::BACKEND_BIGQUERY:
                    $this->assertContains(
                        $e->getCode(),
                        [400, 401],
                        sprintf(
                            'Unexpected error message from Bigquery code: "%s" message: "%s".',
                            $e->getCode(),
                            $e->getMessage(),
                        ),
                    );
                    break;
                default:
                    $this->fail(sprintf(
                        'Unexpected exception for backend "%s". code: "%s",message: "%s"',
                        $workspace['backend'],
                        $e->getCode(),
                        $e->getMessage(),
                    ));
            }
        } catch (\Exception $e) {
            // check that exception not caused by the above fail()
            $this->assertEquals(2, $e->getCode(), $e->getMessage());
        }

        if (!empty($dropOptions['async'])) {
            $afterJobs = $this->listWorkspaceJobs($workspace['id']);
            $job = reset($afterJobs);
            $this->assertEquals('workspaceDrop', $job['operationName']);
        }
    }

    /**
     * @dataProvider dropOptions
     */
    public function testDropNonExistingWorkspace(array $dropOptions, bool $async): void
    {
        if ($async === false) {
            $this->allowTestForBackendsOnly(
                [self::BACKEND_SNOWFLAKE],
                'Test sync actions only on Snowflake',
            );
        }
        $workspaces = new Workspaces($this->workspaceSapiClient);

        try {
            $workspaces->deleteWorkspace(0, $dropOptions, $async);
            $this->fail('exception should be thrown');
        } catch (ClientException $e) {
            $this->assertEquals('workspace.workspaceNotFound', $e->getStringCode());
        }
    }

    /**
     * @return Generator<string, array{options:array{async?:bool},async:bool}>
     */
    public function dropOptions(): \Generator
    {
        yield 'defaults async' => ['options' => [], 'async' => true];
        yield 'defaults sync' => ['options' => [], 'async' => false];
        yield 'legacy options async' => ['options' => ['async' => true], 'async' => false];
        yield 'legacy options sync' => ['options' => ['async' => false], 'async' => true];
    }

    /**
     * @return string
     */
    private function getColumnAmountType(string $backend): string
    {
        $columnAmountType = 'VARCHAR';
        switch ($backend) {
            case self::BACKEND_SNOWFLAKE:
                $columnAmountType = 'NUMBER';
                break;
            case self::BACKEND_BIGQUERY:
                $columnAmountType = 'STRING';
                break;
        }
        return $columnAmountType;
    }

    public function testCreateWorkspaceDoesNotContainPassword(): void
    {
        $url = 'workspaces?' . http_build_query(['async' => true]);

        $result = $this->_client->apiPostJson($url);
        // check that password is not present in the response for standard SNFLK project
        $this->assertArrayNotHasKey('password', $result['connection']);

        // cleanup
        $ws = new Workspaces($this->_client);
        $ws->deleteWorkspace($result['id']);
    }
}
