<?php
/**
 * Created by PhpStorm.
 * User: martinhalamicek
 * Date: 03/05/16
 * Time: 09:45
 */
namespace Keboola\Test\Backend\Mixed;
use Keboola\Db\Import\Snowflake\Connection;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Workspaces;
use Keboola\Test\StorageApiTestCase;

class WorkspacesTest extends StorageApiTestCase
{
    public function setUp()
    {
        parent::setUp();
        $this->_initEmptyTestBuckets();
        $this->clearWorkspaces();
    }

    private function clearWorkspaces()
    {
        $workspaces = new Workspaces($this->_client);
        foreach ($workspaces->listWorkspaces() as $workspace) {
            $workspaces->deleteWorkspace($workspace['id']);
        }
    }

    public function testCreateWorkspaceForMysqlBackendShouldNotBeAllowed()
    {
        $workspaces = new Workspaces($this->_client);

        try {
            $workspaces->createWorkspace([
                'backend' => self::BACKEND_MYSQL,
            ]);
            $this->fail('Mysql workspace should not be created');
        } catch (ClientException $e) {
            $this->assertEquals('backend.notSupported', $e->getStringCode());
        }
    }

    /**
     * @dataProvider  workspaceBackendsData
     * @param $backend
     */
    public function testCreateWorkspaceParam($backend)
    {
        $workspaces = new Workspaces($this->_client);

        $workspace = $workspaces->createWorkspace([
            'backend' => $backend,
        ]);
        $this->assertEquals($backend, $workspace['connection']['backend']);

        $connection = $workspace['connection'];
        if ($backend === self::BACKEND_SNOWFLAKE) {
            $db = new Connection([
                'host' => $connection['host'],
                'database' => $connection['database'],
                'warehouse' => $connection['warehouse'],
                'user' => $connection['user'],
                'password' => $connection['password'],
            ]);

            $db->query("USE SCHEMA " . $db->quoteIdentifier($connection['schema']));

            $schemaNames = array_map(function($schema) {
                return $schema['name'];
            }, $db->fetchAll("SHOW SCHEMAS"));

            $this->assertArrayHasKey($connection['schema'], array_flip($schemaNames));

            // try create a table in workspace
            $db->query("CREATE TABLE mytable (amount NUMBER);");

        } else {
            //redshift connection
            $db = new \PDO(
                "pgsql:dbname={$connection['database']};port=5439;host=" . $connection['host'],
                $connection['user'],
                $connection['password']
            );

            // try create a table in workspace
            $db->query("CREATE TABLE mytable (amount NUMBER);");
        }
    }

    public function workspaceBackendsData()
    {
        return [
            [self::BACKEND_REDSHIFT],
            [self::BACKEND_SNOWFLAKE]
        ];
    }

}