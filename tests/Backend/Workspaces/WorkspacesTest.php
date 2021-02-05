<?php
/**
 * Created by PhpStorm.
 * User: martinhalamicek
 * Date: 03/05/16
 * Time: 09:45
 */
namespace Keboola\Test\Backend\Workspaces;

use Doctrine\DBAL\DBALException;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Workspaces;
use Keboola\Test\Backend\WorkspaceConnectionTrait;
use Keboola\Test\Backend\Workspaces\Backend\SynapseWorkspaceBackend;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;

class WorkspacesTest extends ParallelWorkspacesTestCase
{
    use WorkspaceConnectionTrait;

    public function testWorkspaceCreate()
    {

        $workspaces = new Workspaces($this->workspaceSapiClient);

        $runId = $this->_client->generateRunId();
        $this->_client->setRunId($runId);
        $this->workspaceSapiClient->setRunId($runId);

        $workspace = $workspaces->createWorkspace();
        $connection = $workspace['connection'];

        $tokenInfo = $this->_client->verifyToken();
        $this->assertEquals($tokenInfo['owner']['defaultBackend'], $connection['backend']);

        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        $backend->createTable("mytable", ["amount" => ($connection['backend'] === self::BACKEND_SNOWFLAKE) ? "NUMBER" : "VARCHAR"]);

        $tableNames = $backend->getTables();
        $backend = null; // force odbc disconnect

        $this->assertArrayHasKey("mytable", array_flip($tableNames));

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
        $this->assertCredentialsShouldNotWork($connection);
    }

    public function testWorkspacePasswordReset()
    {
        $workspaces = new Workspaces($this->workspaceSapiClient);

        $workspace = $workspaces->createWorkspace();

        $connection = $workspace['connection'];

        $tokenInfo = $this->_client->verifyToken();
        $this->assertEquals($tokenInfo['owner']['defaultBackend'], $connection['backend']);

        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        $backend->createTable("mytable", ["amount" => ($connection['backend'] === self::BACKEND_SNOWFLAKE) ? "NUMBER" : "VARCHAR"]);

        $tableNames = $backend->getTables();

        $this->assertArrayHasKey("mytable", array_flip($tableNames));

        $runId = $this->_client->generateRunId();
        $this->_client->setRunId($runId);
        $this->workspaceSapiClient->setRunId($runId);

        $newCredentials = $workspaces->resetWorkspacePassword($workspace['id']);
        $this->assertArrayHasKey("password", $newCredentials);

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

        $workspace['connection']['password'] = $newCredentials['password'];
        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        $tableNames = $backend->getTables();
        $backend = null; // force odbc disconnect

        $this->assertArrayHasKey("mytable", array_flip($tableNames));
    }

    /**
     * @dataProvider  dropOptions
     * @param $dropOptions
     */
    public function testDropWorkspace($dropOptions)
    {
        $workspaces = new Workspaces($this->workspaceSapiClient);

        $runId = $this->_client->generateRunId();
        $this->workspaceSapiClient->setRunId($runId);

        $workspace = $workspaces->createWorkspace();
        $connection = $workspace['connection'];

        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);
        $backend->createTable("mytable", ["amount" => ($connection['backend'] === self::BACKEND_SNOWFLAKE) ? "NUMBER" : "VARCHAR"]);

        // sync delete
        $workspaces->deleteWorkspace($workspace['id'], $dropOptions);

        try {
            $backend->countRows("mytable");
            $this->fail("workspace no longer exists. connection should be dead.");
        } catch (\PDOException $e) { // catch redshift connection exception
            $this->assertEquals("57P01", $e->getCode());
        } catch (DBALException $e) {
            // Synapse
            $this->assertEquals(0, $e->getCode(), $e->getMessage());
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
     * @param $dropOptions
     */
    public function testDropNonExistingWorkspace($dropOptions)
    {
        $workspaces = new Workspaces($this->workspaceSapiClient);

        try {
            $workspaces->deleteWorkspace(0, $dropOptions);
            $this->fail('exception should be thrown');
        } catch (ClientException $e) {
            $this->assertEquals('workspace.workspaceNotFound', $e->getStringCode());
        }
    }

    public function dropOptions()
    {
        return [
            [
                []
            ],
            [
                [
                    'async' => true,
                ]
            ]
        ];
    }

    /**
     * @param array $connection
     * @throws \Exception
     */
    private function assertCredentialsShouldNotWork($connection)
    {
        try {
            $this->getDbConnection($connection);
            $this->fail('Credentials should be invalid');
        } catch (\Doctrine\DBAL\Driver\PDOException $e) {
            // Synapse
            if (!in_array(
                $e->getCode(),
                [
                    //https://docs.microsoft.com/en-us/sql/odbc/reference/appendixes/appendix-a-odbc-error-codes?view=sql-server-ver15
                    '28000', // Invalid authorization specification
                    '08004', // Server rejected the connection
                ],
                true
            )) {
                $this->fail(sprintf('Unexpected error code "%s" for Synapse credentials fail.', $e->getCode()));
            }
        } catch (\PDOException $e) {
            // RS
            $this->assertEquals(7, $e->getCode());
        } catch (\Keboola\Db\Import\Exception $e) {
            $this->assertContains('Incorrect username or password was specified', $e->getMessage());
        }
    }
}
