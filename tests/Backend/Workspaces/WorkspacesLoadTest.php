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

    public function testWorkspaceTablesPermissions()
    {
        $workspaces = new Workspaces($this->_client);

        $workspace = $workspaces->createWorkspace();

        //setup test tables
        $tableId = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN), 'languages',
            new CsvFile(__DIR__ . '/../../_data/languages.csv')
        );

        $workspaces->loadWorkspaceData($workspace['id'], [
            'input' => [
                [
                    'source' => $tableId,
                    'destination' => 'languages',
                ],
                [
                    'source' => $tableId,
                    'destination' => 'langs',
                ]
            ],
        ]);

        $db = $this->getDbConnection($workspace['connection']);

        $db->query('DROP TABLE ' . $db->quoteIdentifier('languages'));

        $tables = $db->fetchAll("SHOW TABLES");
        $this->assertCount(1, $tables);
        $this->assertEquals('langs', reset($tables)['name']);
    }

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

        $input = array($mapping1, $mapping2);

        // test if job is created and listed
        $initialJobs = $this->_client->listJobs();
        $workspaces->loadWorkspaceData($workspace['id'], array("input" => $input));
        $afterJobs = $this->_client->listJobs();

        $this->assertEquals('workspaceLoad', $afterJobs[0]['operationName']);
        $this->assertNotEquals($initialJobs[0]['id'], $afterJobs[0]['id']);

        $db = $this->getDbConnection($connection);

        $tableNames = array_map(function ($table) {
            return $table['name'];
        }, $db->fetchAll(sprintf("SHOW TABLES IN SCHEMA %s", $db->quoteIdentifier($connection["schema"]))));

        // check that the tables are in the workspace
        $tables = array_flip($tableNames);
        $this->assertCount(2, array_keys($tables));
        $this->assertArrayHasKey("languagesLoaded", $tables);
        $this->assertArrayHasKey("numbersLoaded", $tables);

        // now we'll load another table and use the preserve parameters to check that all tables are present
        $mapping3 = array("source" => $table1_id, "destination" => "table3");
        $workspaces->loadWorkspaceData($workspace['id'], array("input" => array($mapping3), "preserve" => true));

        $tableNames = array_map(function ($table) {
            return $table['name'];
        }, $db->fetchAll(sprintf("SHOW TABLES IN SCHEMA %s", $db->quoteIdentifier($connection["schema"]))));

        $tables = array_flip($tableNames);
        $this->assertCount(3, array_keys($tables));
        $this->assertArrayHasKey("table3", $tables);
        $this->assertArrayHasKey("languagesLoaded", $tables);
        $this->assertArrayHasKey("numbersLoaded", $tables);

        // now we'll try the same load, but it should clear the workspace first (preserve is false by default)
        $workspaces->loadWorkspaceData($workspace['id'], array("input" => array($mapping3)));
        $tableNames = array_map(function ($table) {
            return $table['name'];
        }, $db->fetchAll(sprintf("SHOW TABLES IN SCHEMA %s", $db->quoteIdentifier($connection["schema"]))));
        $tables = array_flip($tableNames);
        $this->assertCount(1, array_keys($tables));
        $this->assertArrayHasKey("table3", $tables);
    }

    public function testDuplicateDestination()
    {
        $workspaces = new Workspaces($this->_client);
        $workspace = $workspaces->createWorkspace();

        //setup test tables
        $table1_id = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN), 'languages',
            new CsvFile(__DIR__ . '/../../_data/languages.csv')
        );
        $table2_id = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN), 'numbers',
            new CsvFile(__DIR__ . '/../../_data/numbers.csv')
        );

        // now let's try and load 2 different sources to the same destination, this request should be rejected
        $mapping1 = array("source" => $table1_id, "destination" => "languagesLoaded");
        $mapping2 = array("source" => $table2_id, "destination" => "languagesLoaded");
        $inputDupFail = array($mapping1, $mapping2);

        try {
            $workspaces->loadWorkspaceData($workspace['id'], array("input" => $inputDupFail));
            $this->fail('Attempt to write two sources to same destination should fail');
        } catch (ClientException $e) {
            $this->assertEquals('workspace.duplicateDestination', $e->getStringCode());
        }
    }

    public function testSourceTableNotFound()
    {
        $workspaces = new Workspaces($this->_client);
        $workspace = $workspaces->createWorkspace();

        // let's try loading from a table that doesn't exist
        $mappingInvalidSource = array("source" => "in.c-nonExistentBucket.fakeTable", "destination" => "whatever");
        $input404 = array($mappingInvalidSource);
        try {
            $workspaces->loadWorkspaceData($workspace['id'], array("input" => $input404));
            $this->fail('Source does not exist, this should fail');
        } catch (ClientException $e) {
            $this->assertEquals(404, $e->getCode());
            $this->assertEquals('workspace.sourceNotFound', $e->getStringCode());
        }
    }

    public function testInvalidInputs()
    {
        $workspaces = new Workspaces($this->_client);

        $workspace = $workspaces->createWorkspace();

        //setup test tables
        $table1_id = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN), 'languages',
            new CsvFile(__DIR__ . '/../../_data/languages.csv')
        );

        $mapping1 = array("source" => $table1_id, "destination" => "languagesLoaded");
        $input = array($mapping1);

        // test for invalid workspace id
        try {
            $workspaces->loadWorkspaceData(0, array("input" => $input));
            $this->fail('Should not be able to find a workspace with id 0');
        } catch (ClientException $e) {
            $this->assertEquals(404, $e->getCode());
            $this->assertEquals('workspace.workspaceNotFound', $e->getStringCode());
        }

        // test invalid input parameter
        try {
            $workspaces->loadWorkspaceData($workspace['id'], $input);
            $this->fail('Should return bad request, input is required');
        } catch (ClientException $e) {
            $this->assertEquals('workspace.loadRequestInputRequired', $e->getStringCode());
        }

        try {
            $workspaces->loadWorkspaceData($workspace['id'], array("input" => array(array("source" => $table1_id))));
            $this->fail('Should return bad request, destination is required');
        } catch (ClientException $e) {
            $this->assertEquals('workspace.loadRequestBadInput', $e->getStringCode());
        }
        try {
            $workspaces->loadWorkspaceData($workspace['id'], array("input" => array(array("destination" => "destination"))));
            $this->fail('Should return bad request, destination is required');
        } catch (ClientException $e) {
            $this->assertEquals('workspace.loadRequestBadInput', $e->getStringCode());
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
            $this->assertEquals(403, $e->getCode());
            $this->assertEquals('workspace.tableAccessDenied', $e->getStringCode());
        }

    }
}