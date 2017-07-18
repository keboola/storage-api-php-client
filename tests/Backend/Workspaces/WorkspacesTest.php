<?php
/**
 * Created by PhpStorm.
 * User: martinhalamicek
 * Date: 03/05/16
 * Time: 09:45
 */
namespace Keboola\Test\Backend\Workspaces;

use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Workspaces;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;

class WorkspacesTest extends WorkspacesTestCase
{

    public function testWorkspaceCreate()
    {

        $workspaces = new Workspaces($this->_client);

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

        // credentials should not work anymore
        try {
            $this->getDbConnection($connection);
            $this->fail('Credentials should be deleted');
        } catch (\PDOException $e) {
            $this->assertEquals(7, $e->getCode());
        } catch (\Keboola\Db\Import\Exception $e) {
            $this->assertContains('Incorrect username or password was specified', $e->getMessage());
        }
    }

    public function testWorkspacePasswordReset()
    {
        $workspaces = new Workspaces($this->_client);

        $workspace = $workspaces->createWorkspace();

        $connection = $workspace['connection'];

        $tokenInfo = $this->_client->verifyToken();
        $this->assertEquals($tokenInfo['owner']['defaultBackend'], $connection['backend']);

        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        $backend->createTable("mytable", ["amount" => ($connection['backend'] === self::BACKEND_SNOWFLAKE) ? "NUMBER" : "VARCHAR"]);

        $tableNames = $backend->getTables();

        $this->assertArrayHasKey("mytable", array_flip($tableNames));

        $newCredentials = $workspaces->resetWorkspacePassword($workspace['id']);
        $this->assertArrayHasKey("password", $newCredentials);

        if ($connection['backend'] === self::BACKEND_REDSHIFT) {
			try {
				$backend->getTables();
				$this->fail('Connection session should be terminated by server');
			} catch (\PDOException $e) {
				$this->assertEquals('57P01', $e->getCode());
			}
		}

        $backend = null; // force odbc disconnect

        // credentials should not work anymore
        try {
            $this->getDbConnection($connection);
            $this->fail('Credentials should be invalid');
        } catch (\PDOException $e) {
            $this->assertEquals(7, $e->getCode());
        } catch (\Keboola\Db\Import\Exception $e) {
            $this->assertContains('Incorrect username or password was specified', $e->getMessage());
        }

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
        $workspaces = new Workspaces($this->_client);

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
        } catch (\Exception $e) {
            // check that exception not caused by the above fail()
            $this->assertEquals(2, $e->getCode(), $e->getMessage());
        }

        if (!empty($dropOptions['async'])) {
            $job = $this->_client->listJobs()[0];
            $this->assertEquals('workspaceDrop', $job['operationName']);
            $this->assertEquals($workspace['id'], $job['operationParams']['workspaceId']);
        }
    }

    /**
     * @dataProvider dropOptions
     * @param $dropOptions
     */
    public function testDropNonExistingWorkspace($dropOptions)
    {
        $workspaces = new Workspaces($this->_client);

        try {
            $workspaces->deleteWorkspace('fake', $dropOptions);
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
}
