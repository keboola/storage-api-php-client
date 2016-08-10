<?php
/**
 * Created by PhpStorm.
 * User: martinhalamicek
 * Date: 03/05/16
 * Time: 09:45
 */
namespace Keboola\Test\Backend\Workspaces;

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
        } catch (\Exception $e) {
        }
    }


}