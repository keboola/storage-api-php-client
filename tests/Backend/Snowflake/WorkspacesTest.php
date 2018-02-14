<?php
/**
 * Created by PhpStorm.
 * User: martinhalamicek
 * Date: 03/05/16
 * Time: 09:45
 */
namespace Keboola\Test\Backend\Snowflake;

use Keboola\Csv\CsvFile;
use Keboola\Db\Import\Snowflake\Connection;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Workspaces;
use Keboola\Test\StorageApiTestCase;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;

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

    public function testLoadToWorkspaceAtOffset()
    {
        $workspaces = new Workspaces($this->_client);
        $workspace = $workspaces->createWorkspace();

        //setup test tables
        $sourceTableId = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN),
            'languages',
            new CsvFile(__DIR__ . '/../../_data/languages.csv')
        );
        sleep(25);

        $this->_client->writeTable(
            $sourceTableId,
            new CsvFile(__DIR__ . '/../../_data/languages.increment.csv'),
            [
                'incremental' => true,
            ]
        );

        $mapping = array(
            "source" => $sourceTableId,
            "destination" => "offsetTestLoaded",
            "timeOffset" => -20
        );

        $input = array($mapping);

        $workspaces->loadWorkspaceData($workspace['id'], array("input" => $input));

        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        $tables = $backend->getTables();

        // check that the tables are in the workspace
        $this->assertCount(1, $tables);
        $this->assertContains($backend->toIdentifier("offsetTestLoaded"), $tables);

        // check table structure and data
        $data = $backend->fetchAll("offsetTestLoaded", \PDO::FETCH_ASSOC);
        $this->assertCount(2, $data[0], 'there should be two columns');
        $this->assertArrayHasKey('id', $data[0]);
        $this->assertArrayHasKey('name', $data[0]);
        $this->assertCount(5, $data); // there should only be the original
        $this->assertArrayEqualsSorted(Client::parseCsv(file_get_contents(__DIR__ . '/../../_data/languages.csv'), true, ",", '"'), $data, 'id');

    }
}
