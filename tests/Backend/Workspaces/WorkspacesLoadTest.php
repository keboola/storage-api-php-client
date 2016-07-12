<?php
/**
 * Created by PhpStorm.
 * User: marc
 * Date: 08/07/2016
 * Time: 15:30
 */

namespace Keboola\Test\Backend\Workspaces;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Workspaces;
use Keboola\StorageApi\ClientException;

class WorkspaceLoadTest extends WorkspacesTestCase
{
    public function testWorkspaceLoad()
    {
        $workspaces = new Workspaces($this->_client);

        $workspace = $workspaces->createWorkspace();
        $connection = $workspace['connection'];

        //setup test tables
        $table1_id = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN), 'languages',
            new CsvFile(__DIR__ . '/../../_data/languages.csv')
        );

        $table2_id = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN), 'numbers',
            new CsvFile(__DIR__ . '/../../_data/numbers.csv')
        );

        $mapping1 = array("source" => $table1_id, "destination" => "languagesLoaded");
        $mapping2 = array("source" => $table2_id, "destination" => "numbersLoaded");
        $mapping3 = array("source" => $table2_id, "destination" => "languagesLoaded");

        $input = array($mapping1, $mapping2);

        $workspaces->loadWorkspaceData($workspace['id'],array("input" => $input, "preserve" => false));

        $db = $this->getDbConnection($connection);

        $db->query("USE SCHEMA " . $db->quoteIdentifier($connection['schema']));

        $tableNames = array_map(function($table) {
            return $table['name'];
        }, $db->fetchAll(sprintf("SHOW TABLES IN SCHEMA %s", $db->quoteIdentifier($connection["schema"]))));

        // check that the tables are in the workspace
        $tables = array_flip($tableNames);
        $this->assertArrayHasKey("languagesLoaded", $tables);
        $this->assertArrayHasKey("numbersLoaded", $tables);

        $inputDupFail = array($mapping1, $mapping3);
        try {
            $workspaces->loadWorkspaceData($workspace['id'], array("input" => $inputDupFail));
            $this->fail('Attempt to write two sources to same destination should fail');
        } catch (ClientException $e) {

        }
    }

    public function testInvalidBucketPermissions()
    {
        // make a test table
        $tableId = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN), 'languages',
            new CsvFile(__DIR__ . '/../../_data/languages.csv')
        );

        $bucketPermissions = array(
            $this->getTestBucketId(self::STAGE_OUT) => 'read',
        );
        $tokenId = $this->_client->createToken($bucketPermissions, 'workspaceLoadTest: Out read token');
        $token = $this->_client->getToken($tokenId);

        $testClient = new \Keboola\StorageApi\Client(array(
            'token' => $token['token'],
            'url' => STORAGE_API_URL
        ));

        // create the workspace with the limited permission client
        $workspaces = new Workspaces($testClient);
        $workspace = $workspaces->createWorkspace();

        $input = array(
            array(
                "source" => $tableId,
                "destination" => "irrelevant"
            )
        );
        try {
            $workspaces->loadWorkspaceData($workspace['id'], array("input" => $input));
            $this->fail("This should fail due to insufficient permission");
        } catch (ClientException $e) {

        }

    }
}