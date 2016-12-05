<?php
/**
 * Created by PhpStorm.
 * User: martinhalamicek
 * Date: 03/05/16
 * Time: 09:45
 */
namespace Keboola\Test\Backend\Snowflake;

use Keboola\Test\StorageApiTestCase;
use Keboola\StorageApi\Workspaces;
use Keboola\Db\Import\Snowflake\Connection;

class WorkspacesTest extends StorageApiTestCase
{
    public function setUp()
    {
        parent::setUp();
        $this->_initEmptyTestBuckets();
        $this->deleteAllWorkspaces();
    }

    private function deleteAllWorkspaces()
    {
        $workspaces = new Workspaces($this->_client);
        foreach ($workspaces->listWorkspaces() as $workspace) {
            $workspaces->deleteWorkspace($workspace['id']);
        }
    }

    public function testWorkspaceCreateWithStatementTimeout()
    {
        $timeoutValue = 3;
        $workspaces = new Workspaces($this->_client);

        $workspace = $workspaces->createWorkspace([
            'statementTimeoutSeconds' => $timeoutValue,
        ]);

        $this->assertEquals($timeoutValue, $workspace['statementTimeoutSeconds']);
        $connection = $workspace['connection'];

        $db = new Connection([
            'host' => $connection['host'],
            'database' => $connection['database'],
            'warehouse' => $connection['warehouse'],
            'user' => $connection['user'],
            'password' => $connection['password'],
        ]);
        // set connection to use workspace schema
        $db->query(sprintf("USE SCHEMA %s;", $db->quoteIdentifier($connection['schema'])));


        $params = $db->fetchAll("SHOW PARAMETERS LIKE 'STATEMENT_TIMEOUT_IN_SECONDS'");
        $this->assertCount(1, $params);

        $timeoutParam = reset($params);
        $this->assertEquals($timeoutValue, (int) $timeoutParam['value']);
    }
}
