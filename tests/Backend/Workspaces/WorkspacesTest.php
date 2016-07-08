<?php
/**
 * Created by PhpStorm.
 * User: martinhalamicek
 * Date: 03/05/16
 * Time: 09:45
 */
namespace Keboola\Test\Backend\Workspaces;

use Keboola\StorageApi\Workspaces;
use Keboola\Csv\CsvFile;

class WorkspacesTest extends WorkSpaceTestCase
{
    public function testWorkspaceLoad()
    {
        $workspaces = new Workspaces($this->_client);

        $workspace = $workspaces->createWorkspace();
        $connection = $workspace['connection'];

        //setup test bucket
        $tableId = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN), 'languagesTest',
            new CsvFile(__DIR__ . '/../../_data/languages.csv')
        );
        
        $source = $tableId;

        $workspaces->loadWorkspaceData($workspace['id'],array("source"=>$source, "destination" => "happyTable"));

        $db = $this->getDbConnection($connection);

        $db->query("USE SCHEMA " . $db->quoteIdentifier($connection['schema']));

        $tableNames = array_map(function($table) {
            return $table['name'];
        }, $db->fetchAll(sprintf("SHOW TABLES IN SCHEMA %s", $db->quoteIdentifier($connection["schema"]))));

        $this->assertArrayHasKey("happyTable", array_flip($tableNames));

    }

    public function testWorkspaceCreate()
    {

        $workspaces = new Workspaces($this->_client);

        $workspace = $workspaces->createWorkspace();
        $connection = $workspace['connection'];
        
        $db = $this->getDbConnection($connection);
        
        if ($connection['backend'] === parent::BACKEND_SNOWFLAKE) {

            $schemaNames = array_map(function($schema) {
                return $schema['name'];
            }, $db->fetchAll("SHOW SCHEMAS"));

            $this->assertArrayHasKey($connection['schema'], array_flip($schemaNames));

            $db->query("USE SCHEMA " . $db->quoteIdentifier($connection['schema']));

            // try create a table in the workspace
            $db->query("CREATE TABLE \"mytable\" (amount NUMBER);");

            $tableNames = array_map(function($table) {
               return $table['name'];
            }, $db->fetchAll(sprintf("SHOW TABLES IN SCHEMA %s", $db->quoteIdentifier($connection["schema"]))));

            $this->assertArrayHasKey("mytable", array_flip($tableNames));
        } else {

            // try create a table in workspace
            $db->query("CREATE TABLE mytable (amount NUMBER);");

        }

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
            $this->getDbConnection($connection);
            $this->fail('Credentials should be deleted');
        } catch (\Exception $e) {
        }
    }

    
}