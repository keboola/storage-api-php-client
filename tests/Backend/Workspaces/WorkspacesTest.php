<?php
/**
 * Created by PhpStorm.
 * User: martinhalamicek
 * Date: 03/05/16
 * Time: 09:45
 */
namespace Keboola\Test\Backend\Workspaces;
use Keboola\Db\Import\Snowflake\Connection;
use Keboola\StorageApi\Workspaces;
use Keboola\Csv\CsvFile;
use Keboola\Test\StorageApiTestCase;

class WorkspacesTest extends StorageApiTestCase
{
    public function setUp()
    {
        parent::setUp();
        $this->_initEmptyTestBuckets();
    }


    public function testWorkspaceCreate()
    {
        $workspaces = new Workspaces($this->_client);

        $workspace = $workspaces->createWorkspace();
        $connection = $workspace['connection'];

        $snowflakeConnection = new Connection([
            'host' => $connection['host'],
            'database' => $connection['database'],
            'warehouse' => $connection['warehouse'],
            'user' => $connection['user'],
            'password' => $connection['password'],
        ]);

        $snowflakeConnection->query("USE SCHEMA " . $snowflakeConnection->quoteIdentifier($connection['schema']));

        $schemaNames = array_map(function($schema) {
            return $schema['name'];
        }, $snowflakeConnection->fetchAll("SHOW SCHEMAS"));

        $this->assertArrayHasKey($connection['schema'], array_flip($schemaNames));

        // try create a table in workspace
        $snowflakeConnection->query("CREATE TABLE mytable (amount NUMBER);");
        
        // get workspace
        $workspace = $workspaces->getWorkspace($workspace['id']);
        $this->assertArrayNotHasKey('password',  $workspace['connection']);
        
        // list workspaces
        $workspacesIds = array_map(function($workspace) {
            return $workspace['id'];
        },  $workspaces->listWorkspaces());

        $this->assertArrayHasKey($workspace['id'], array_flip($workspacesIds));

        $workspaces->deleteWorkspace($workspace['id']);

        // credentials should not work anymore
        try {
            new Connection([
                'host' => $connection['host'],
                'database' => $connection['database'],
                'warehouse' => $connection['warehouse'],
                'user' => $connection['user'],
                'password' => $connection['password'],
            ]);
            $this->fail('Credentials should be deleted');
        } catch (\Exception $e) {
        }
    }


}