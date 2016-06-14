<?php
/**
 *
 * User: Martin Halamíček
 * Date: 16.5.12
 * Time: 11:46
 *
 */

namespace Keboola\Test\Backend\Workspaces;
use Keboola\Db\Import\Snowflake\Connection;
use Keboola\StorageApi\Workspaces;
use Keboola\Test\StorageApiTestCase;

class CopyImportTest extends StorageApiTestCase
{

    public function setUp()
    {
        parent::setUp();
        $this->_initEmptyTestBuckets();
        $this->deleteAllWorkspaces();
    }

    private function deleteAllWorkspaces()
    {
        $workspaces  = new Workspaces($this->_client);
        foreach ($workspaces->listWorkspaces() as $workspace) {
            $workspaces->deleteWorkspace($workspace['id']);
        }
    }

    public function testCreateTableFromWorkspace()
    {
        // create workspace and source table in workspace
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

        $snowflakeConnection->query("create table \"test.Languages3\" (
			\"Id\" integer not null,
			\"Name\" varchar not null
		);");
        $snowflakeConnection->query("insert into \"test.Languages3\" (\"Id\", \"Name\") values (1, 'cz'), (2, 'en');");


        // create table from workspace
        $tableId = $this->_client->createTableAsyncDirect($this->getTestBucketId(self::STAGE_IN), array(
            'name' => 'languages',
            'dataWorkspaceId' => $workspace['id'],
            'dataTableName' => 'test.Languages3',
        ));

        $expected = array(
            '"Id","Name"',
            '"1","cz"',
            '"2","en"',
        );

        $this->assertLinesEqualsSorted(implode("\n", $expected) . "\n", $this->_client->exportTable($tableId, null, array(
            'format' => 'rfc',
        )), 'imported data comparsion');
    }

    public function testCopyImport()
    {
        $table = $this->_client->apiPost("storage/buckets/" . $this->getTestBucketId(self::STAGE_IN) . "/tables", array(
            'dataString' => 'Id,Name,update',
            'name' => 'languages',
            'primaryKey' => 'Id',
        ));

        // create workspace and source table in workspace
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

        $snowflakeConnection->query("create table \"test.Languages3\" (
			\"Id\" integer not null,
			\"Name\" varchar not null,
			\"update\" varchar
		);");

        $snowflakeConnection->query("insert into \"test.Languages3\" (\"Id\", \"Name\") values (1, 'cz'), (2, 'en');");

        $this->_client->writeTableAsyncDirect($table['id'], array(
            'dataWorkspaceId' => $workspace['id'],
            'dataTableName' => 'test.Languages3',
        ));

        $expected = array(
            '"Id","Name","update"',
            '"1","cz",""',
            '"2","en",""',
        );

        $this->assertLinesEqualsSorted(implode("\n", $expected) . "\n", $this->_client->exportTable($table['id'], null, array(
            'format' => 'rfc',
        )), 'imported data comparsion');


        $snowflakeConnection->query("truncate \"test.Languages3\"");
        $snowflakeConnection->query("insert into \"test.Languages3\" values (1, 'cz', '1'), (3, 'sk', '1');");

        $this->_client->writeTableAsyncDirect($table['id'], array(
            'dataWorkspaceId' => $workspace['id'],
            'dataTableName' => 'test.Languages3',
            'incremental' => true,
        ));

        $expected = array(
            '"Id","Name","update"',
            '"1","cz","1"',
            '"2","en",""',
            '"3","sk","1"',
        );
        $this->assertLinesEqualsSorted(implode("\n", $expected) . "\n", $this->_client->exportTable($table['id'], null, array(
            'format' => 'rfc',
        )), 'previously null column updated');

        $snowflakeConnection->query("truncate table \"test.Languages3\"");
        $snowflakeConnection->query("alter table \"test.Languages3\" ADD COLUMN \"new_col\" varchar");
        $snowflakeConnection->query("insert into \"test.Languages3\" values (1, 'cz', '1', null), (3, 'sk', '1', 'newValue');");

        $this->_client->writeTableAsyncDirect($table['id'], array(
            'dataWorkspaceId' => $workspace['id'],
            'dataTableName' => 'test.Languages3',
            'incremental' => true,
        ));

        $expected = array(
            '"Id","Name","update","new_col"',
            '"1","cz","1",""',
            '"2","en","",""',
            '"3","sk","1","newValue"',
        );
        $this->assertLinesEqualsSorted(implode("\n", $expected) . "\n", $this->_client->exportTable($table['id'], null, array(
            'format' => 'rfc',
        )), 'new  column added');
    }


}