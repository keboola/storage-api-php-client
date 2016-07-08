<?php
/**
 * Created by PhpStorm.
 * User: marc
 * Date: 08/07/2016
 * Time: 15:30
 */

namespace Keboola\Test\Backend\Workspaces;

use Keboola\Csv\CsvFile;

class WorkspaceLoadTest extends WorkspaceTestCase
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
}